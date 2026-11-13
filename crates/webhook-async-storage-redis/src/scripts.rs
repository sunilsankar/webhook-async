use std::sync::LazyLock;

use redis::Script;

pub(crate) static ENQUEUE_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
-- KEYS: [job_key, all_jobs_key, state_queued_key, hook_key, priorities_key, priority_queue_key, idemp_key]
-- ARGV: [id, has_idemp, now_rfc, idemp_expires_rfc, job_prefix, created_ms, available_ms, priority_str, k1, v1, ...]
if ARGV[2] == '1' then
  local raw = redis.call('GET', KEYS[7])
  if raw then
    local sep = string.find(raw, '|', 1, true)
    if sep then
      local existing_id = string.sub(raw, 1, sep - 1)
      local existing_exp = string.sub(raw, sep + 1)
      if existing_exp > ARGV[3] then
        local existing_job = redis.call('HGETALL', ARGV[5] .. existing_id)
        if #existing_job > 0 then
          return {'DEDUP', existing_job}
        end
      end
    end
  end
end

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {'ERR', 'DUPLICATE_ID'}
end

redis.call('HSET', KEYS[1], unpack(ARGV, 9))
redis.call('ZADD', KEYS[2], ARGV[6], ARGV[1])
redis.call('ZADD', KEYS[3], ARGV[6], ARGV[1])
redis.call('ZADD', KEYS[4], ARGV[6], ARGV[1])
redis.call('ZADD', KEYS[5], -tonumber(ARGV[8]), ARGV[8])
redis.call('ZADD', KEYS[6], ARGV[7], ARGV[1])

if ARGV[2] == '1' then
  redis.call('SET', KEYS[7], ARGV[1] .. '|' .. ARGV[4])
end

return {'CREATED'}
"#,
    )
});

pub(crate) static CLAIM_DUE_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
-- KEYS: [priorities_key, state_queued_key, state_running_key, leases_key]
-- ARGV: [now_ms, now_rfc, lease_ms, lease_rfc, worker_id, job_prefix, q_prefix, limit, tok_1, ...]
local priorities = redis.call('ZRANGE', KEYS[1], 0, -1)
local limit = tonumber(ARGV[8])
local claimed = {}
local tok_idx = 9

for _, p_str in ipairs(priorities) do
  if #claimed >= limit then break end
  local q_key = ARGV[7] .. p_str
  local skipped = 0
  local page_size = (limit - #claimed) + 32

  while #claimed < limit do
    local cands = redis.call('ZRANGEBYSCORE', q_key, '-inf', ARGV[1], 'LIMIT', skipped, page_size)
    if #cands == 0 then break end
    for _, id in ipairs(cands) do
      if #claimed >= limit then break end
      local job_key = ARGV[6] .. id
      local avail = redis.call('HGET', job_key, 'available_at')
      local state = redis.call('HGET', job_key, 'state')
      if state == 'queued' and avail and avail <= ARGV[2] then
        local tok = ARGV[tok_idx]
        tok_idx = tok_idx + 1
        local created_ms = tonumber(redis.call('HGET', job_key, 'created_at_ms'))
        redis.call('ZREM', q_key, id)
        redis.call('ZREM', KEYS[2], id)
        redis.call('ZADD', KEYS[3], created_ms, id)
        redis.call('ZADD', KEYS[4], ARGV[3], id)
        local started = redis.call('HGET', job_key, 'started_at')
        if not started or started == '' then
          redis.call('HSET', job_key, 'started_at', ARGV[2])
        end
        redis.call('HSET', job_key,
          'state', 'running',
          'worker_id', ARGV[5],
          'lease_token', tok,
          'lease_expires_at', ARGV[4],
          'heartbeat_at', ARGV[2],
          'updated_at', ARGV[2]
        )
        table.insert(claimed, redis.call('HGETALL', job_key))
      else
        skipped = skipped + 1
      end
    end
    if #cands < page_size then break end
  end

  if redis.call('ZCARD', q_key) == 0 then
    redis.call('ZREM', KEYS[1], p_str)
  end
end

return claimed
"#,
    )
});

pub(crate) static HEARTBEAT_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
-- KEYS: [job_key, leases_key]
-- ARGV: [id, lease_token, now_rfc, lease_ms, lease_rfc]
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'ERR', 'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'running' and state ~= 'cancel_requested' then
  return {'ERR', 'INVALID_TRANSITION', state or ''}
end
local tok = redis.call('HGET', KEYS[1], 'lease_token')
if tok ~= ARGV[2] then
  return {'ERR', 'OWNERSHIP_MISMATCH'}
end
redis.call('HSET', KEYS[1],
  'heartbeat_at', ARGV[3],
  'lease_expires_at', ARGV[5],
  'updated_at', ARGV[3]
)
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[1])
return {'OK', redis.call('HGETALL', KEYS[1])}
"#,
    )
});

pub(crate) static ACK_SUCCESS_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
-- KEYS: [job_key, attempts_key, leases_key, state_running_key, state_cancel_req_key, state_succeeded_key]
-- ARGV: [id, lease_token, attempt_id, finished_rfc, exit_code_str, has_out, out_str, has_err, err_str]
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'ERR', 'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'running' and state ~= 'cancel_requested' then
  return {'ERR', 'INVALID_TRANSITION', state or ''}
end
local tok = redis.call('HGET', KEYS[1], 'lease_token')
if tok ~= ARGV[2] then
  return {'ERR', 'OWNERSHIP_MISMATCH'}
end

local created_ms = tonumber(redis.call('HGET', KEYS[1], 'created_at_ms'))
local worker_id = redis.call('HGET', KEYS[1], 'worker_id') or 'unknown'
local started_at = redis.call('HGET', KEYS[1], 'started_at') or ARGV[4]
local next_attempt = (tonumber(redis.call('HGET', KEYS[1], 'attempt')) or 0) + 1

if state == 'running' then
  redis.call('ZREM', KEYS[4], ARGV[1])
else
  redis.call('ZREM', KEYS[5], ARGV[1])
end
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZADD', KEYS[6], created_ms, ARGV[1])

redis.call('HDEL', KEYS[1],
  'worker_id', 'lease_token', 'lease_expires_at', 'heartbeat_at',
  'next_retry_at', 'error_class', 'error_summary', 'stdout_preview', 'stderr_preview'
)
redis.call('HSET', KEYS[1],
  'state', 'succeeded',
  'attempt', tostring(next_attempt),
  'completed_at', ARGV[4],
  'updated_at', ARGV[4],
  'exit_code', ARGV[5]
)
if ARGV[6] == '1' then redis.call('HSET', KEYS[1], 'stdout_preview', ARGV[7]) end
if ARGV[8] == '1' then redis.call('HSET', KEYS[1], 'stderr_preview', ARGV[9]) end

local attempt_obj = {
  job_id = ARGV[1],
  attempt = next_attempt,
  worker_id = worker_id,
  started_at = started_at,
  finished_at = ARGV[4],
  exit_code = tonumber(ARGV[5])
}
if ARGV[6] == '1' then attempt_obj.stdout_preview = ARGV[7] end
if ARGV[8] == '1' then attempt_obj.stderr_preview = ARGV[9] end
redis.call('RPUSH', KEYS[2], cjson.encode(attempt_obj))

return {'OK', redis.call('HGETALL', KEYS[1])}
"#,
    )
});

pub(crate) static ACK_FAILURE_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
-- KEYS: [job_key, attempts_key, leases_key, state_running_key, state_cancel_req_key, state_target_key, retry_wait_key]
-- ARGV: [id, lease_token, attempt_id, target_state, finished_rfc, next_retry_rfc, next_retry_ms, error_class, error_summary, has_exit, exit_str, has_out, out_str, has_err, err_str]
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'ERR', 'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'running' and state ~= 'cancel_requested' then
  return {'ERR', 'INVALID_TRANSITION', state or ''}
end
local tok = redis.call('HGET', KEYS[1], 'lease_token')
if tok ~= ARGV[2] then
  return {'ERR', 'OWNERSHIP_MISMATCH'}
end
if state == 'cancel_requested' and ARGV[4] == 'retry_wait' then
  return {'ERR', 'INVALID_TRANSITION', state}
end

local created_ms = tonumber(redis.call('HGET', KEYS[1], 'created_at_ms'))
local worker_id = redis.call('HGET', KEYS[1], 'worker_id') or 'unknown'
local started_at = redis.call('HGET', KEYS[1], 'started_at') or ARGV[5]
local next_attempt = (tonumber(redis.call('HGET', KEYS[1], 'attempt')) or 0) + 1

if state == 'running' then
  redis.call('ZREM', KEYS[4], ARGV[1])
else
  redis.call('ZREM', KEYS[5], ARGV[1])
end
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZADD', KEYS[6], created_ms, ARGV[1])

redis.call('HDEL', KEYS[1],
  'worker_id', 'lease_token', 'lease_expires_at', 'heartbeat_at',
  'next_retry_at', 'completed_at', 'exit_code', 'stdout_preview', 'stderr_preview'
)
redis.call('HSET', KEYS[1],
  'state', ARGV[4],
  'attempt', tostring(next_attempt),
  'updated_at', ARGV[5],
  'error_class', ARGV[8],
  'error_summary', ARGV[9]
)
if ARGV[4] == 'retry_wait' then
  redis.call('HSET', KEYS[1], 'next_retry_at', ARGV[6], 'available_at', ARGV[6])
  redis.call('ZADD', KEYS[7], ARGV[7], ARGV[1])
else
  redis.call('HSET', KEYS[1], 'completed_at', ARGV[5])
end
if ARGV[10] == '1' then redis.call('HSET', KEYS[1], 'exit_code', ARGV[11]) end
if ARGV[12] == '1' then redis.call('HSET', KEYS[1], 'stdout_preview', ARGV[13]) end
if ARGV[14] == '1' then redis.call('HSET', KEYS[1], 'stderr_preview', ARGV[15]) end

local attempt_obj = {
  job_id = ARGV[1],
  attempt = next_attempt,
  worker_id = worker_id,
  started_at = started_at,
  finished_at = ARGV[5],
  error_class = ARGV[8],
  error_summary = ARGV[9]
}
if ARGV[10] == '1' then attempt_obj.exit_code = tonumber(ARGV[11]) end
if ARGV[12] == '1' then attempt_obj.stdout_preview = ARGV[13] end
if ARGV[14] == '1' then attempt_obj.stderr_preview = ARGV[15] end
redis.call('RPUSH', KEYS[2], cjson.encode(attempt_obj))

return {'OK', redis.call('HGETALL', KEYS[1])}
"#,
    )
});

pub(crate) static ACK_CANCELLED_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
-- KEYS: [job_key, attempts_key, leases_key, state_running_key, state_cancel_req_key, state_cancelled_key]
-- ARGV: [id, lease_token, attempt_id, cancelled_rfc, error_summary, has_out, out_str, has_err, err_str]
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'ERR', 'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'running' and state ~= 'cancel_requested' then
  return {'ERR', 'INVALID_TRANSITION', state or ''}
end
local tok = redis.call('HGET', KEYS[1], 'lease_token')
if tok ~= ARGV[2] then
  return {'ERR', 'OWNERSHIP_MISMATCH'}
end

local created_ms = tonumber(redis.call('HGET', KEYS[1], 'created_at_ms'))
local worker_id = redis.call('HGET', KEYS[1], 'worker_id') or 'unknown'
local started_at = redis.call('HGET', KEYS[1], 'started_at') or ARGV[4]
local next_attempt = (tonumber(redis.call('HGET', KEYS[1], 'attempt')) or 0) + 1

if state == 'running' then
  redis.call('ZREM', KEYS[4], ARGV[1])
else
  redis.call('ZREM', KEYS[5], ARGV[1])
end
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZADD', KEYS[6], created_ms, ARGV[1])

redis.call('HDEL', KEYS[1],
  'worker_id', 'lease_token', 'lease_expires_at', 'heartbeat_at',
  'next_retry_at', 'exit_code', 'stdout_preview', 'stderr_preview'
)
redis.call('HSET', KEYS[1],
  'state', 'cancelled',
  'attempt', tostring(next_attempt),
  'completed_at', ARGV[4],
  'updated_at', ARGV[4],
  'error_class', 'cancelled',
  'error_summary', ARGV[5]
)
if ARGV[6] == '1' then redis.call('HSET', KEYS[1], 'stdout_preview', ARGV[7]) end
if ARGV[8] == '1' then redis.call('HSET', KEYS[1], 'stderr_preview', ARGV[9]) end

local attempt_obj = {
  job_id = ARGV[1],
  attempt = next_attempt,
  worker_id = worker_id,
  started_at = started_at,
  finished_at = ARGV[4],
  error_class = 'cancelled',
  error_summary = ARGV[5]
}
if ARGV[6] == '1' then attempt_obj.stdout_preview = ARGV[7] end
if ARGV[8] == '1' then attempt_obj.stderr_preview = ARGV[9] end
redis.call('RPUSH', KEYS[2], cjson.encode(attempt_obj))

return {'OK', redis.call('HGETALL', KEYS[1])}
"#,
    )
});

pub(crate) static PROMOTE_RETRIES_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
-- KEYS: [retry_wait_key, state_retry_wait_key, state_queued_key, priorities_key]
-- ARGV: [now_ms, now_rfc, job_prefix, q_prefix]
local promoted = 0
local skipped = 0
local page_size = 128

while true do
  local cands = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', skipped, page_size)
  if #cands == 0 then break end
  for _, id in ipairs(cands) do
    local job_key = ARGV[3] .. id
    local state = redis.call('HGET', job_key, 'state')
    local next_retry = redis.call('HGET', job_key, 'next_retry_at')
    if state == 'retry_wait' and next_retry and next_retry <= ARGV[2] then
      local created_ms = tonumber(redis.call('HGET', job_key, 'created_at_ms'))
      local p_str = redis.call('HGET', job_key, 'priority') or '0'
      redis.call('ZREM', KEYS[1], id)
      redis.call('ZREM', KEYS[2], id)
      redis.call('ZADD', KEYS[3], created_ms, id)
      redis.call('ZADD', KEYS[4], -tonumber(p_str), p_str)
      redis.call('ZADD', ARGV[4] .. p_str, ARGV[1], id)
      redis.call('HDEL', job_key, 'next_retry_at')
      redis.call('HSET', job_key,
        'state', 'queued',
        'available_at', ARGV[2],
        'updated_at', ARGV[2]
      )
      promoted = promoted + 1
    else
      skipped = skipped + 1
    end
  end
  if #cands < page_size then break end
end

return promoted
"#,
    )
});

pub(crate) static RECOVER_EXPIRED_LEASES_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
-- KEYS: [leases_key, state_running_key, state_cancel_req_key, state_queued_key, state_cancelled_key, priorities_key]
-- ARGV: [now_ms, now_rfc, job_prefix, q_prefix]
local recovered = {}
local skipped = 0
local page_size = 128

while true do
  local cands = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', skipped, page_size)
  if #cands == 0 then break end
  for _, id in ipairs(cands) do
    local job_key = ARGV[3] .. id
    local state = redis.call('HGET', job_key, 'state')
    local lease_exp = redis.call('HGET', job_key, 'lease_expires_at')
    if (state == 'running' or state == 'cancel_requested') and lease_exp and lease_exp <= ARGV[2] then
      local created_ms = tonumber(redis.call('HGET', job_key, 'created_at_ms'))
      redis.call('ZREM', KEYS[1], id)
      redis.call('HDEL', job_key, 'worker_id', 'lease_token', 'lease_expires_at', 'heartbeat_at')
      if state == 'running' then
        local p_str = redis.call('HGET', job_key, 'priority') or '0'
        redis.call('ZREM', KEYS[2], id)
        redis.call('ZADD', KEYS[4], created_ms, id)
        redis.call('ZADD', KEYS[6], -tonumber(p_str), p_str)
        redis.call('ZADD', ARGV[4] .. p_str, ARGV[1], id)
        redis.call('HSET', job_key,
          'state', 'queued',
          'available_at', ARGV[2],
          'updated_at', ARGV[2],
          'error_summary', 'lease expired; returned to queue'
        )
      else
        redis.call('ZREM', KEYS[3], id)
        redis.call('ZADD', KEYS[5], created_ms, id)
        redis.call('HSET', job_key,
          'state', 'cancelled',
          'completed_at', ARGV[2],
          'updated_at', ARGV[2],
          'error_class', 'cancelled',
          'error_summary', 'lease expired during cancellation'
        )
      end
      table.insert(recovered, id)
    else
      skipped = skipped + 1
    end
  end
  if #cands < page_size then break end
end

return recovered
"#,
    )
});

pub(crate) static REQUEST_CANCEL_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
-- KEYS: [job_key, priorities_key, retry_wait_key, state_queued_key, state_retry_wait_key, state_running_key, state_cancel_req_key, state_cancelled_key]
-- ARGV: [id, now_rfc, q_prefix]
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'ERR', 'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[1], 'state')
local created_ms = tonumber(redis.call('HGET', KEYS[1], 'created_at_ms'))

if state == 'queued' or state == 'retry_wait' then
  if state == 'queued' then
    local p_str = redis.call('HGET', KEYS[1], 'priority') or '0'
    local q_key = ARGV[3] .. p_str
    redis.call('ZREM', q_key, ARGV[1])
    if redis.call('ZCARD', q_key) == 0 then
      redis.call('ZREM', KEYS[2], p_str)
    end
    redis.call('ZREM', KEYS[4], ARGV[1])
  else
    redis.call('ZREM', KEYS[3], ARGV[1])
    redis.call('ZREM', KEYS[5], ARGV[1])
  end
  redis.call('ZADD', KEYS[8], created_ms, ARGV[1])
  redis.call('HDEL', KEYS[1], 'next_retry_at')
  redis.call('HSET', KEYS[1],
    'state', 'cancelled',
    'completed_at', ARGV[2],
    'updated_at', ARGV[2],
    'error_class', 'cancelled',
    'error_summary', 'cancelled while queued'
  )
  return {'OK', redis.call('HGETALL', KEYS[1])}
elseif state == 'running' or state == 'cancel_requested' then
  if state == 'running' then
    redis.call('ZREM', KEYS[6], ARGV[1])
    redis.call('ZADD', KEYS[7], created_ms, ARGV[1])
  end
  redis.call('HSET', KEYS[1],
    'state', 'cancel_requested',
    'updated_at', ARGV[2]
  )
  return {'OK', redis.call('HGETALL', KEYS[1])}
else
  return {'ERR', 'INVALID_TRANSITION', state or ''}
end
"#,
    )
});

pub(crate) static RETRY_JOB_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
-- KEYS: [job_key, priorities_key, state_failed_key, state_dead_letter_key, state_queued_key]
-- ARGV: [id, now_ms, now_rfc, q_prefix]
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'ERR', 'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'failed' and state ~= 'dead_letter' then
  return {'ERR', 'INVALID_TRANSITION', state or ''}
end

local created_ms = tonumber(redis.call('HGET', KEYS[1], 'created_at_ms'))
local p_str = redis.call('HGET', KEYS[1], 'priority') or '0'
local attempt = tonumber(redis.call('HGET', KEYS[1], 'attempt')) or 0
local max_attempts = tonumber(redis.call('HGET', KEYS[1], 'max_attempts')) or 1
if max_attempts <= attempt then
  max_attempts = attempt + 1
end

if state == 'failed' then
  redis.call('ZREM', KEYS[3], ARGV[1])
else
  redis.call('ZREM', KEYS[4], ARGV[1])
end
redis.call('ZADD', KEYS[5], created_ms, ARGV[1])
redis.call('ZADD', KEYS[2], -tonumber(p_str), p_str)
redis.call('ZADD', ARGV[4] .. p_str, ARGV[2], ARGV[1])

redis.call('HDEL', KEYS[1],
  'worker_id', 'lease_token', 'lease_expires_at', 'heartbeat_at',
  'completed_at', 'next_retry_at', 'exit_code',
  'stdout_preview', 'stderr_preview', 'error_class', 'error_summary'
)
redis.call('HSET', KEYS[1],
  'state', 'queued',
  'available_at', ARGV[3],
  'updated_at', ARGV[3],
  'max_attempts', tostring(max_attempts)
)

return {'OK', redis.call('HGETALL', KEYS[1])}
"#,
    )
});
