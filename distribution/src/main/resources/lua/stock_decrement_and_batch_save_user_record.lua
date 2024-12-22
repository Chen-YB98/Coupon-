-- 定义最大值和位数
local SECOND_FIELD_BITS = 13

-- 将两个字段组合成一个int
local function combineFields(firstField, secondField)
    local firstFieldValue = firstField and 1 or 0
    return (firstFieldValue * 2 ^ SECOND_FIELD_BITS) + secondField
end

-- Lua脚本开始
local key = KEYS[1] -- Redis Key
local userSetKey = KEYS[2] -- 用户领券 Set 的 Key
local userIdAndRowNum = ARGV[1] -- 用户 ID 和 Excel 所在行数

-- 获取库存
local stock = tonumber(redis.call('HGET', key, 'stock'))

-- 检查库存是否大于0
if stock == nil or stock <= 0 then
    return combineFields(false, redis.call('SCARD', userSetKey))
end

-- 自减库存
redis.call('HINCRBY', key, 'stock', -1)

-- 添加用户到领券集合
redis.call('SADD', userSetKey, userIdAndRowNum)

-- 获取用户领券集合的长度
local userSetLength = redis.call('SCARD', userSetKey)

-- 返回结果
return combineFields(true, userSetLength)
