/*
 * 牛券（oneCoupon）优惠券平台项目
 *
 * 版权所有 (C) [2024-至今] [山东流年网络科技有限公司]
 *
 * 保留所有权利。
 *
 * 1. 定义和解释
 *    本文件（包括其任何修改、更新和衍生内容）是由[山东流年网络科技有限公司]及相关人员开发的。
 *    "软件"指的是与本文件相关的任何代码、脚本、文档和相关的资源。
 *
 * 2. 使用许可
 *    本软件的使用、分发和解释均受中华人民共和国法律的管辖。只有在遵守以下条件的前提下，才允许使用和分发本软件：
 *    a. 未经[山东流年网络科技有限公司]的明确书面许可，不得对本软件进行修改、复制、分发、出售或出租。
 *    b. 任何未授权的复制、分发或修改都将被视为侵犯[山东流年网络科技有限公司]的知识产权。
 *
 * 3. 免责声明
 *    本软件按"原样"提供，没有任何明示或暗示的保证，包括但不限于适销性、特定用途的适用性和非侵权性的保证。
 *    在任何情况下，[山东流年网络科技有限公司]均不对任何直接、间接、偶然、特殊、典型或间接的损害（包括但不限于采购替代商品或服务；使用、数据或利润损失）承担责任。
 *
 * 4. 侵权通知与处理
 *    a. 如果[山东流年网络科技有限公司]发现或收到第三方通知，表明存在可能侵犯其知识产权的行为，公司将采取必要的措施以保护其权利。
 *    b. 对于任何涉嫌侵犯知识产权的行为，[山东流年网络科技有限公司]可能要求侵权方立即停止侵权行为，并采取补救措施，包括但不限于删除侵权内容、停止侵权产品的分发等。
 *    c. 如果侵权行为持续存在或未能得到妥善解决，[山东流年网络科技有限公司]保留采取进一步法律行动的权利，包括但不限于发出警告信、提起民事诉讼或刑事诉讼。
 *
 * 5. 其他条款
 *    a. [山东流年网络科技有限公司]保留随时修改这些条款的权利。
 *    b. 如果您不同意这些条款，请勿使用本软件。
 *
 * 未经[山东流年网络科技有限公司]的明确书面许可，不得使用此文件的任何部分。
 *
 * 本软件受到[山东流年网络科技有限公司]及其许可人的版权保护。
 */

package com.nageoffer.onecoupon.engine.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.map.MapUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.nageoffer.onecoupon.engine.common.constant.EngineRedisConstant;
import com.nageoffer.onecoupon.engine.common.enums.CouponTemplateStatusEnum;
import com.nageoffer.onecoupon.engine.dao.entity.CouponTemplateDO;
import com.nageoffer.onecoupon.engine.dao.mapper.CouponTemplateMapper;
import com.nageoffer.onecoupon.engine.dao.sharding.DBShardingUtil;
import com.nageoffer.onecoupon.engine.dto.req.CouponTemplateQueryReqDTO;
import com.nageoffer.onecoupon.engine.dto.resp.CouponTemplateQueryRespDTO;
import com.nageoffer.onecoupon.engine.service.CouponTemplateService;
import com.nageoffer.onecoupon.framework.exception.ClientException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 优惠券模板业务逻辑实现层
 * <p>
 * 作者：马丁
 * 加项目群：早加入就是优势！500人内部项目群，分享的知识总有你需要的 <a href="https://t.zsxq.com/cw7b9" />
 * 开发时间：2024-07-08
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CouponTemplateServiceImpl extends ServiceImpl<CouponTemplateMapper, CouponTemplateDO> implements CouponTemplateService {

    private final CouponTemplateMapper couponTemplateMapper;
    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private final RBloomFilter<String> couponTemplateQueryBloomFilter;

    // 布隆过滤器 + 缓存空对象 + 分布式锁（双重判定空值）
    @Override
    public CouponTemplateQueryRespDTO findCouponTemplate(CouponTemplateQueryReqDTO requestParam) {
        // 查询 Redis 缓存中是否存在优惠券模板信息
        String couponTemplateCacheKey = String.format(EngineRedisConstant.COUPON_TEMPLATE_KEY, requestParam.getCouponTemplateId());
        Map<Object, Object> couponTemplateCacheMap = stringRedisTemplate.opsForHash().entries(couponTemplateCacheKey);

        // 如果存在直接返回，不存在需要通过布隆过滤器、缓存空值以及双重判定锁的形式读取数据库中的记录
        if (MapUtil.isEmpty(couponTemplateCacheMap)) {
            // 判断布隆过滤器是否存在指定模板 ID，不存在直接返回错误
            if (!couponTemplateQueryBloomFilter.contains(requestParam.getCouponTemplateId())) {
                throw new ClientException("优惠券模板不存在");
            }

            // 查询 Redis 缓存中是否存在优惠券模板空值信息，如果有代表模板不存在，直接返回
            String couponTemplateIsNullCacheKey = String.format(EngineRedisConstant.COUPON_TEMPLATE_IS_NULL_KEY, requestParam.getCouponTemplateId());
            Boolean hasKeyFlag = stringRedisTemplate.hasKey(couponTemplateIsNullCacheKey);
            if (hasKeyFlag) {
                throw new ClientException("优惠券模板不存在");
            }

            // 获取优惠券模板分布式锁
            // 关于缓存击穿更多注释事项，欢迎查看我的B站视频：https://www.bilibili.com/video/BV1qz421z7vC
            RLock lock = redissonClient.getLock(String.format(EngineRedisConstant.LOCK_COUPON_TEMPLATE_KEY, requestParam.getCouponTemplateId()));
            lock.lock();

            try {
                // 双重判定空值缓存是否存在，存在则继续抛异常
                hasKeyFlag = stringRedisTemplate.hasKey(couponTemplateIsNullCacheKey);
                if (hasKeyFlag) {
                    throw new ClientException("优惠券模板不存在");
                }

                // 通过双重判定锁优化大量请求无意义查询数据库
                couponTemplateCacheMap = stringRedisTemplate.opsForHash().entries(couponTemplateCacheKey);
                if (MapUtil.isEmpty(couponTemplateCacheMap)) {
                    LambdaQueryWrapper<CouponTemplateDO> queryWrapper = Wrappers.lambdaQuery(CouponTemplateDO.class)
                            .eq(CouponTemplateDO::getShopNumber, Long.parseLong(requestParam.getShopNumber()))
                            .eq(CouponTemplateDO::getId, Long.parseLong(requestParam.getCouponTemplateId()))
                            .eq(CouponTemplateDO::getStatus, CouponTemplateStatusEnum.ACTIVE.getStatus());
                    CouponTemplateDO couponTemplateDO = couponTemplateMapper.selectOne(queryWrapper);

                    // 优惠券模板不存在或者已过期加入空值缓存，并且抛出异常
                    if (couponTemplateDO == null) {
                        stringRedisTemplate.opsForValue().set(couponTemplateIsNullCacheKey, "", 30, TimeUnit.MINUTES);
                        throw new ClientException("优惠券模板不存在或已过期");
                    }

                    // 通过将数据库的记录序列化成 JSON 字符串放入 Redis 缓存
                    CouponTemplateQueryRespDTO actualRespDTO = BeanUtil.toBean(couponTemplateDO, CouponTemplateQueryRespDTO.class);
                    Map<String, Object> cacheTargetMap = BeanUtil.beanToMap(actualRespDTO, false, true);
                    Map<String, String> actualCacheTargetMap = cacheTargetMap.entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry -> entry.getValue() != null ? entry.getValue().toString() : ""
                            ));

                    // 通过 LUA 脚本执行设置 Hash 数据以及设置过期时间
                    String luaScript = "redis.call('HMSET', KEYS[1], unpack(ARGV, 1, #ARGV - 1)) " +
                            "redis.call('EXPIREAT', KEYS[1], ARGV[#ARGV])";

                    List<String> keys = Collections.singletonList(couponTemplateCacheKey);
                    List<String> args = new ArrayList<>(actualCacheTargetMap.size() * 2 + 1);
                    actualCacheTargetMap.forEach((key, value) -> {
                        args.add(key);
                        args.add(value);
                    });

                    // 优惠券活动过期时间转换为秒级别的 Unix 时间戳
                    args.add(String.valueOf(couponTemplateDO.getValidEndTime().getTime() / 1000));

                    // 执行 LUA 脚本
                    stringRedisTemplate.execute(
                            new DefaultRedisScript<>(luaScript, Long.class),
                            keys,
                            args.toArray()
                    );
                    couponTemplateCacheMap = cacheTargetMap.entrySet()
                            .stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                }
            } finally {
                lock.unlock();
            }
        }

        return BeanUtil.mapToBean(couponTemplateCacheMap, CouponTemplateQueryRespDTO.class, false, CopyOptions.create());
    }

    @Override
    public List<CouponTemplateDO> listCouponTemplateByIds(List<Long> couponTemplateIds, List<Long> shopNumbers) {
        // 1. 将 shopNumbers集合 对应的index拆分到数据库中
        Map<Integer, List<Long>> databaseIndexMap = splitIndexByDatabase(shopNumbers);

        List<CouponTemplateDO> result = new ArrayList<>();
        // 2. 对每个数据库执行查询
        for (Map.Entry<Integer, List<Long>> entry : databaseIndexMap.entrySet()) {
            List<Long> shopNumbersSubset = entry.getValue();

            // 执行查询
            List<CouponTemplateDO> couponTemplateDOList = queryDatabase(couponTemplateIds, shopNumbersSubset);
            result.addAll(couponTemplateDOList);
        }

        return result;
    }

    private List<CouponTemplateDO> queryDatabase(List<Long> couponTemplateIds, List<Long> shopNumbers) {
        LambdaQueryWrapper<CouponTemplateDO> queryWrapper = Wrappers.lambdaQuery(CouponTemplateDO.class)
                .in(CouponTemplateDO::getShopNumber, shopNumbers)
                .in(CouponTemplateDO::getId, couponTemplateIds);
        return couponTemplateMapper.selectList(queryWrapper);
    }

    private Map<Integer, List<Long>> splitIndexByDatabase(List<Long> shopNumbers) {
        Map<Integer, List<Long>> databaseShopNumberMap = new HashMap<>();

        for (Long shopNumber : shopNumbers) {
            int databaseMod = DBShardingUtil.doCouponSharding(shopNumber);
            databaseShopNumberMap
                    .computeIfAbsent(databaseMod, k -> new ArrayList<>())
                    .add(shopNumber);
        }

        return databaseShopNumberMap;
    }

    /**
     * 缓存击穿解决方案
     */
    public CouponTemplateQueryRespDTO findCouponTemplateV1(CouponTemplateQueryReqDTO requestParam) {
        // 查询 Redis 缓存中是否存在优惠券模板信息
        String couponTemplateCacheKey = String.format(EngineRedisConstant.COUPON_TEMPLATE_KEY, requestParam.getCouponTemplateId());
        Map<Object, Object> couponTemplateCacheMap = stringRedisTemplate.opsForHash().entries(couponTemplateCacheKey);

        // 如果存在直接返回，不存在需要通过双重判定锁的形式读取数据库中的记录
        if (MapUtil.isEmpty(couponTemplateCacheMap)) {
            // 获取优惠券模板分布式锁
            // 关于缓存击穿更多注释事项，欢迎查看我的B站视频：https://www.bilibili.com/video/BV1qz421z7vC
            RLock lock = redissonClient.getLock(String.format(EngineRedisConstant.LOCK_COUPON_TEMPLATE_KEY, requestParam.getCouponTemplateId()));
            lock.lock();

            try {
                // 通过双重判定锁优化大量请求无意义查询数据库
                couponTemplateCacheMap = stringRedisTemplate.opsForHash().entries(couponTemplateCacheKey);
                if (MapUtil.isEmpty(couponTemplateCacheMap)) {
                    LambdaQueryWrapper<CouponTemplateDO> queryWrapper = Wrappers.lambdaQuery(CouponTemplateDO.class)
                            .eq(CouponTemplateDO::getShopNumber, Long.parseLong(requestParam.getShopNumber()))
                            .eq(CouponTemplateDO::getId, Long.parseLong(requestParam.getCouponTemplateId()))
                            .eq(CouponTemplateDO::getStatus, CouponTemplateStatusEnum.ACTIVE.getStatus());
                    CouponTemplateDO couponTemplateDO = couponTemplateMapper.selectOne(queryWrapper);

                    // 优惠券模板不存在或者已过期直接抛出异常
                    if (couponTemplateDO == null) {
                        throw new ClientException("优惠券模板不存在或已过期");
                    }

                    // 通过将数据库的记录序列化成 JSON 字符串放入 Redis 缓存
                    CouponTemplateQueryRespDTO actualRespDTO = BeanUtil.toBean(couponTemplateDO, CouponTemplateQueryRespDTO.class);
                    Map<String, Object> cacheTargetMap = BeanUtil.beanToMap(actualRespDTO, false, true);
                    Map<String, String> actualCacheTargetMap = cacheTargetMap.entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry -> entry.getValue() != null ? entry.getValue().toString() : ""
                            ));

                    // 通过 LUA 脚本执行设置 Hash 数据以及设置过期时间
                    String luaScript = "redis.call('HMSET', KEYS[1], unpack(ARGV, 1, #ARGV - 1)) " +
                            "redis.call('EXPIREAT', KEYS[1], ARGV[#ARGV])";

                    List<String> keys = Collections.singletonList(couponTemplateCacheKey);
                    List<String> args = new ArrayList<>(actualCacheTargetMap.size() * 2 + 1);
                    actualCacheTargetMap.forEach((key, value) -> {
                        args.add(key);
                        args.add(value);
                    });

                    // 优惠券活动过期时间转换为秒级别的 Unix 时间戳
                    args.add(String.valueOf(couponTemplateDO.getValidEndTime().getTime() / 1000));

                    // 执行 LUA 脚本
                    stringRedisTemplate.execute(
                            new DefaultRedisScript<>(luaScript, Long.class),
                            keys,
                            args.toArray()
                    );
                    couponTemplateCacheMap = cacheTargetMap.entrySet()
                            .stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                }
            } finally {
                lock.unlock();
            }
        }

        return BeanUtil.mapToBean(couponTemplateCacheMap, CouponTemplateQueryRespDTO.class, false, CopyOptions.create());
    }

    /**
     * 缓存穿透解决方案之布隆过滤器
     */
    public CouponTemplateQueryRespDTO findCouponTemplateV2(CouponTemplateQueryReqDTO requestParam) {
        // 判断布隆过滤器是否存在指定模板 ID，不存在直接返回错误
        if (!couponTemplateQueryBloomFilter.contains(requestParam.getCouponTemplateId())) {
            throw new ClientException("优惠券模板不存在");
        }

        LambdaQueryWrapper<CouponTemplateDO> queryWrapper = Wrappers.lambdaQuery(CouponTemplateDO.class)
                .eq(CouponTemplateDO::getShopNumber, Long.parseLong(requestParam.getShopNumber()))
                .eq(CouponTemplateDO::getId, Long.parseLong(requestParam.getCouponTemplateId()))
                .eq(CouponTemplateDO::getStatus, CouponTemplateStatusEnum.ACTIVE.getStatus());
        CouponTemplateDO couponTemplateDO = couponTemplateMapper.selectOne(queryWrapper);

        return BeanUtil.toBean(couponTemplateDO, CouponTemplateQueryRespDTO.class);
    }
}
