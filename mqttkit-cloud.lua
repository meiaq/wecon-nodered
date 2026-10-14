-- =====================================================================
-- mqttkit.lua  |  HMI Wecon (seri ig) -> MQTT -> Node-RED Dashboard
-- Trainer kit "Temperatur Sensor" - panel Digital Temperature
--   D0 : suhu (float 32-bit hasil FLT..DESUB, makan D0+D1)
--   D2 : humidity (angka mentah dari RS, dibagi 10)
--
-- Topik:
--   wecon/kit01/data    -> JSON tiap 1 detik (HMI publish)
--   wecon/kit01/status  -> "online"/"offline" (retain, pakai LWT)
--   wecon/kit01/cmd     -> perintah dari dashboard (HMI subscribe)
--
-- Catatan:
--   * Library mqtt cuma jalan di HMI seri ig, TIDAK jalan di simulator.
--   * Panggil mqttkit_start() dari we_bg_init() di Script_BG.lua.
-- =====================================================================
mqttkit_limits = 11   -- wajib: biar fungsi di file ini kepanggil dari Script_BG.lua

local json = require("json")

-- ---------------------------------------------------------------------
-- 1. SETTING JARINGAN  (ganti IP sesuai PC yang jalanin Node-RED)
-- ---------------------------------------------------------------------
-- Versi CLOUD: broker publik EMQX (gratis, tanpa login, SEMUA ORANG bisa lihat datanya)
local BROKER     = "tcp://broker.emqx.io:1883"
local USE_DOMAIN = 1                       -- 1 = alamat broker berupa nama domain
local CLIENT_ID  = "mi02-mekatronika-kit01" -- harus unik di broker publik
local TOPIC      = "mi02mekatronika/kit01"  -- topik unik biar nggak tabrakan sama orang lain

local CONN = {
  username     = "",                   -- kosongin kalau broker tanpa login
  password     = "",
  keepalive    = 30,                   -- detik
  cleansession = 1,
}

-- Last Will: kalau HMI mati/putus, broker otomatis kirim "offline"
local LWT = { topic = TOPIC .. "/status", message = "offline", qos = 1, retain = 1 }

-- Bit internal HMI buat indikator "MQTT connected" (pasang Bit Lamp ke sini)
local STATUS_BIT = "@B_HDX200.0"

-- ---------------------------------------------------------------------
-- 2. PETA ALAMAT
--    type : "float" = 32-bit float (makan 2 register: D48+D49, D0+D1, dst)
--           "short" = 16-bit signed | "bit" = bit
--    scale: pengali, cuma dipakai kalau type "short" (misal 0.1)
--    Kalau format alamat PLC beda: hapus string-nya, taruh kursor di
--    dalam tanda kutip, tekan Ctrl+W (word) / Ctrl+B (bit) di Lua editor.
-- ---------------------------------------------------------------------
local TAGS = {
  { key = "d_temp", addr = "@W_1#D0", type = "float" },               -- PLC: DESUB D108 K40 D0
  { key = "d_hum",  addr = "@W_1#D2", type = "short", scale = 0.1 },  -- PLC: RS H103 H1 K1 D2
}

-- Perintah dari dashboard yang BOLEH nulis ke PLC (whitelist).
--   value diisi  -> nilai tetap, payload cuma pemicu. Contoh {"reset_max":1}
--   value kosong -> nilai diambil dari payload. Contoh {"setpoint":35.5}
local CMDS = {
  -- contoh: reset = { addr = "@W_1#D300", type = "short", value = 0 },
}

local PUB_MS      = 1000   -- interval publish (ms)
local RETRY_TICKS = 5      -- coba konek ulang tiap 5 x PUB_MS
local TIMER_ID    = 91     -- ID timer global, jangan bentrok sama timer lain

-- ---------------------------------------------------------------------
-- 3. INTERNAL (gak perlu diubah)
-- ---------------------------------------------------------------------
local cli         -- objek mqtt
local wait = 0    -- hitung mundur sebelum coba konek lagi

local function round1(v)              -- 1 desimal, sama kayak tampilan HMI
  return math.floor(v * 10 + 0.5) / 10
end

local function setStatus(on)
  we_bas_setbit(STATUS_BIT, on and 1 or 0)
end

local function readTag(t)
  local v, err
  if t.type == "bit" then
    v, err = we_bas_getbit(t.addr)
  elseif t.type == "float" then
    v, err = we_bas_getfloat(t.addr)
  else
    v, err = we_bas_getshort(t.addr)
  end
  if v == nil then
    print("baca gagal " .. t.addr .. ": " .. tostring(err))
    return nil, tostring(err)          -- error dikirim ke Node-RED di field "err"
  end
  if t.scale then v = v * t.scale end
  if t.type ~= "bit" then v = round1(v) end
  return v
end

local function writeTag(c, val)
  if c.type == "bit" then
    return we_bas_setbit(c.addr, val)
  elseif c.type == "float" then
    return we_bas_setfloat(c.addr, val)
  else
    return we_bas_setshort(c.addr, math.floor(val + 0.5))
  end
end

local function onMessage(topic, msg)
  if topic ~= TOPIC .. "/cmd" then return end
  local ok, data = pcall(json.decode, msg)
  if not ok or type(data) ~= "table" then
    print("cmd bukan JSON: " .. tostring(msg))
    return
  end
  for k, v in pairs(data) do
    local c = CMDS[k]
    if c then
      local val = c.value
      if val == nil then
        val = tonumber(v)
        if val ~= nil and c.scale and c.type ~= "float" then val = val / c.scale end
      end
      if val ~= nil then
        writeTag(c, val)
        print("cmd " .. k .. " -> " .. c.addr .. " = " .. tostring(val))
      end
    else
      print("cmd ditolak (gak ada di whitelist): " .. tostring(k))
    end
  end
end

local function ensureClient()
  if cli then return true end
  local err
  cli, err = mqtt.create(BROKER, CLIENT_ID, USE_DOMAIN)
  if not cli then
    print("mqtt.create gagal: " .. tostring(err))
    return false
  end
  cli:on("message", onMessage)
  cli:on("offline", function(cause)
    print("MQTT putus: " .. tostring(cause))
    setStatus(false)
  end)
  return true
end

local function tryConnect()
  local ok, err = cli:connect(CONN, LWT)
  if not ok then
    print("MQTT konek gagal: " .. tostring(err))
    setStatus(false)
    return false
  end
  cli:subscribe(TOPIC .. "/cmd", 0)                 -- subscribe ulang tiap konek
  cli:publish(TOPIC .. "/status", "online", 1, 1)   -- timpa "offline" yang ke-retain
  setStatus(true)
  print("MQTT tersambung ke " .. BROKER)
  return true
end

local function loop()
  if not ensureClient() then return end

  if not cli:isconnected() then
    setStatus(false)
    if wait > 0 then wait = wait - 1 return end
    if not tryConnect() then
      -- objek lama kadang nggak bisa konek ulang (misal habis broker restart / Deploy),
      -- jadi dibuang dan dibikin baru di percobaan berikutnya
      pcall(function() cli:close() end)
      cli = nil
      wait = RETRY_TICKS
      return
    end
  end

  local data = { ts = os.date("%Y-%m-%d %H:%M:%S") }
  for _, t in ipairs(TAGS) do
    local v, e = readTag(t)
    if e then
      data.err = data.err or {}
      data.err[t.key] = t.addr .. " -> " .. e
    else
      data[t.key] = v
    end
  end

  local ok, err = cli:publish(TOPIC .. "/data", json.encode(data), 0, 0)
  if not ok then print("publish gagal: " .. tostring(err)) end

  -- tiap nilai juga dikirim ke topik sendiri (angka polos) buat aplikasi MQTT di HP:
  --   mi02mekatronika/kit01/d_temp  dan  mi02mekatronika/kit01/d_hum
  for _, t in ipairs(TAGS) do
    if data[t.key] ~= nil then
      cli:publish(TOPIC .. "/" .. t.key, tostring(data[t.key]), 0, 0)
    end
  end
end

-- Dipanggil sekali dari we_bg_init() di Script_BG.lua
function mqttkit_start()
  setStatus(false)
  -- loop dibungkus pcall: kalau ada error, timer tetap hidup dan koneksi dibikin ulang
  timer.create(-1, TIMER_ID, PUB_MS, function()
    local ok, e = pcall(loop)
    if not ok then
      print("loop error: " .. tostring(e))
      if cli then pcall(function() cli:close() end) end
      cli = nil
      setStatus(false)
    end
  end)                                        -- -1 = timer global background
end

-- =====================================================================
-- Isi Script_BG.lua (cukup tambahin baris mqttkit_start()):
--
-- function we_bg_init()
--   mqttkit_start()
-- end
--
-- function we_bg_poll()
-- end
-- =====================================================================
