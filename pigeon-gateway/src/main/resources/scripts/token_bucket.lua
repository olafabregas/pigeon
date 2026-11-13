-- Atomic token bucket.
-- KEYS[1] bucket key
-- ARGV[1] refill rate (tokens/second), ARGV[2] burst capacity, ARGV[3] tokens requested
-- Returns {allowed(1|0), remaining(floor), retry_after_ms}
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = burst
  ts = now_ms
end

local elapsed = math.max(0, now_ms - ts)
tokens = math.min(burst, tokens + (elapsed * rate / 1000.0))

local allowed = 0
local retry_ms = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
else
  if rate > 0 then
    retry_ms = math.ceil((requested - tokens) * 1000.0 / rate)
  else
    retry_ms = 3600000
  end
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now_ms))
-- expire once the bucket would be full again (min 1s)
local ttl_ms = 1000
if rate > 0 then
  ttl_ms = math.max(1000, math.ceil(burst * 1000.0 / rate) * 2)
end
redis.call('PEXPIRE', KEYS[1], ttl_ms)

return {allowed, math.floor(tokens), retry_ms}
