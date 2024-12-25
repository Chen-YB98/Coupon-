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
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Singleton;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.toolkit.SqlHelper;
import com.nageoffer.onecoupon.engine.common.constant.EngineRedisConstant;
import com.nageoffer.onecoupon.engine.common.context.UserContext;
import com.nageoffer.onecoupon.engine.common.enums.RedisStockDecrementErrorEnum;
import com.nageoffer.onecoupon.engine.common.enums.UserCouponStatusEnum;
import com.nageoffer.onecoupon.engine.dao.entity.CouponSettlementDO;
import com.nageoffer.onecoupon.engine.dao.entity.UserCouponDO;
import com.nageoffer.onecoupon.engine.dao.mapper.CouponSettlementMapper;
import com.nageoffer.onecoupon.engine.dao.mapper.CouponTemplateMapper;
import com.nageoffer.onecoupon.engine.dao.mapper.UserCouponMapper;
import com.nageoffer.onecoupon.engine.dto.req.CouponCreatePaymentGoodsReqDTO;
import com.nageoffer.onecoupon.engine.dto.req.CouponCreatePaymentReqDTO;
import com.nageoffer.onecoupon.engine.dto.req.CouponProcessPaymentReqDTO;
import com.nageoffer.onecoupon.engine.dto.req.CouponProcessRefundReqDTO;
import com.nageoffer.onecoupon.engine.dto.req.CouponTemplateQueryReqDTO;
import com.nageoffer.onecoupon.engine.dto.req.CouponTemplateRedeemReqDTO;
import com.nageoffer.onecoupon.engine.dto.resp.CouponTemplateQueryRespDTO;
import com.nageoffer.onecoupon.engine.mq.event.UserCouponDelayCloseEvent;
import com.nageoffer.onecoupon.engine.mq.event.UserCouponRedeemEvent;
import com.nageoffer.onecoupon.engine.mq.producer.UserCouponDelayCloseProducer;
import com.nageoffer.onecoupon.engine.mq.producer.UserCouponRedeemProducer;
import com.nageoffer.onecoupon.engine.service.CouponTemplateService;
import com.nageoffer.onecoupon.engine.service.UserCouponService;
import com.nageoffer.onecoupon.engine.toolkit.StockDecrementReturnCombinedUtil;
import com.nageoffer.onecoupon.framework.exception.ClientException;
import com.nageoffer.onecoupon.framework.exception.ServiceException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;

import static com.nageoffer.onecoupon.engine.common.constant.EngineRedisConstant.USER_COUPON_TEMPLATE_LIST_KEY;

/**
 * 用户优惠券业务逻辑实现层
 * <p>
 * 作者：马丁
 * 加项目群：早加入就是优势！500人内部沟通群，分享的知识总有你需要的 <a href="https://t.zsxq.com/cw7b9" />
 * 开发时间：2024-07-25
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class UserCouponServiceImpl implements UserCouponService {

    private final CouponTemplateService couponTemplateService;
    private final UserCouponMapper userCouponMapper;
    private final CouponTemplateMapper couponTemplateMapper;
    private final CouponSettlementMapper couponSettlementMapper;
    private final UserCouponDelayCloseProducer couponDelayCloseProducer;
    private final UserCouponRedeemProducer userCouponRedeemProducer;

    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private final TransactionTemplate transactionTemplate;

    @Value("${one-coupon.user-coupon-list.save-cache.type:direct}")
    private String userCouponListSaveCacheType;

    private final static String STOCK_DECREMENT_AND_SAVE_USER_RECEIVE_LUA_PATH = "lua/stock_decrement_and_save_user_receive.lua";

    @Override
    public void redeemUserCoupon(CouponTemplateRedeemReqDTO requestParam) {
        // 验证缓存是否存在，保障数据存在并且缓存中存在
        CouponTemplateQueryRespDTO couponTemplate = couponTemplateService.findCouponTemplate(BeanUtil.toBean(requestParam, CouponTemplateQueryReqDTO.class));

        // 验证领取的优惠券是否在活动有效时间
        boolean isInTime = DateUtil.isIn(new Date(), couponTemplate.getValidStartTime(), couponTemplate.getValidEndTime());
        if (!isInTime) {
            // 一般来说优惠券领取时间不到的时候，前端不会放开调用请求，可以理解这是用户调用接口在“攻击”
            throw new ClientException("不满足优惠券领取时间");
        }

        // 获取 LUA 脚本，并保存到 Hutool 的单例管理容器，下次直接获取不需要加载
        DefaultRedisScript<Long> buildLuaScript = Singleton.get(STOCK_DECREMENT_AND_SAVE_USER_RECEIVE_LUA_PATH, () -> {
            DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
            redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource(STOCK_DECREMENT_AND_SAVE_USER_RECEIVE_LUA_PATH)));
            redisScript.setResultType(Long.class);
            return redisScript;
        });

        // 验证用户是否符合优惠券领取条件
        JSONObject receiveRule = JSON.parseObject(couponTemplate.getReceiveRule());
        String limitPerPerson = receiveRule.getString("limitPerPerson");

        // 执行 LUA 脚本进行扣减库存以及增加 Redis 用户领券记录次数
        String couponTemplateCacheKey = String.format(EngineRedisConstant.COUPON_TEMPLATE_KEY, requestParam.getCouponTemplateId());
        String userCouponTemplateLimitCacheKey = String.format(EngineRedisConstant.USER_COUPON_TEMPLATE_LIMIT_KEY, UserContext.getUserId(), requestParam.getCouponTemplateId());
        Long stockDecrementLuaResult = stringRedisTemplate.execute(
                buildLuaScript,
                ListUtil.of(couponTemplateCacheKey, userCouponTemplateLimitCacheKey),
                String.valueOf(couponTemplate.getValidEndTime().getTime()), limitPerPerson
        );

        // 判断 LUA 脚本执行返回类，如果失败根据类型返回报错提示
        long firstField = StockDecrementReturnCombinedUtil.extractFirstField(stockDecrementLuaResult);
        if (RedisStockDecrementErrorEnum.isFail(firstField)) {
            throw new ServiceException(RedisStockDecrementErrorEnum.fromType(firstField));
        }

        // 通过编程式事务执行优惠券库存自减以及增加用户优惠券领取记录
        long extractSecondField = StockDecrementReturnCombinedUtil.extractSecondField(stockDecrementLuaResult);
        transactionTemplate.executeWithoutResult(status -> {
            try {
                int decremented = couponTemplateMapper.decrementCouponTemplateStock(Long.parseLong(requestParam.getShopNumber()), Long.parseLong(requestParam.getCouponTemplateId()), 1L);
                if (!SqlHelper.retBool(decremented)) {
                    throw new ServiceException("优惠券已被领取完啦");
                }

                // 添加 Redis 用户领取的优惠券记录列表
                Date now = new Date();
                DateTime validEndTime = DateUtil.offsetHour(now, JSON.parseObject(couponTemplate.getConsumeRule()).getInteger("validityPeriod"));
                UserCouponDO userCouponDO = UserCouponDO.builder()
                        .couponTemplateId(Long.parseLong(requestParam.getCouponTemplateId()))
                        .userId(Long.parseLong(UserContext.getUserId()))
                        .source(requestParam.getSource())
                        .receiveCount(Long.valueOf(extractSecondField).intValue())
                        .status(UserCouponStatusEnum.UNUSED.getCode())
                        .receiveTime(now)
                        .validStartTime(now)
                        .validEndTime(validEndTime)
                        .build();
                userCouponMapper.insert(userCouponDO);

                // 保存优惠券缓存集合有两个选项：direct 在流程里直接操作，binlog 通过解析数据库日志后操作
                if (StrUtil.equals(userCouponListSaveCacheType, "direct")) {
                    // 添加用户领取优惠券模板缓存记录
                    String userCouponListCacheKey = String.format(EngineRedisConstant.USER_COUPON_TEMPLATE_LIST_KEY, UserContext.getUserId());
                    String userCouponItemCacheKey = StrUtil.builder()
                            .append(requestParam.getCouponTemplateId())
                            .append("_")
                            .append(userCouponDO.getId())
                            .toString();
                    stringRedisTemplate.opsForZSet().add(userCouponListCacheKey, userCouponItemCacheKey, now.getTime());

                    // 由于 Redis 在持久化或主从复制的极端情况下可能会出现数据丢失，而我们对指令丢失几乎无法容忍，因此我们采用经典的写后查询策略来应对这一问题
                    Double scored;
                    try {
                        scored = stringRedisTemplate.opsForZSet().score(userCouponListCacheKey, userCouponItemCacheKey);
                        // scored 为空意味着可能 Redis Cluster 主从同步丢失了数据，比如 Redis 主节点还没有同步到从节点就宕机了，解决方案就是再新增一次
                        if (scored == null) {
                            // 如果这里也新增失败了怎么办？我们大概率做不到绝对的万无一失，只能尽可能增加成功率
                            stringRedisTemplate.opsForZSet().add(userCouponListCacheKey, userCouponItemCacheKey, now.getTime());
                        }
                    } catch (Throwable ex) {
                        log.warn("查询Redis用户优惠券记录为空或抛异常，可能Redis宕机或主从复制数据丢失，基础错误信息：{}", ex.getMessage());
                        // 如果直接抛异常大概率 Redis 宕机了，所以应该写个延时队列向 Redis 重试放入值。为了避免代码复杂性，这里直接写新增，大家知道最优解决方案即可
                        stringRedisTemplate.opsForZSet().add(userCouponListCacheKey, userCouponItemCacheKey, now.getTime());
                    }

                    // 发送延时消息队列，等待优惠券到期后，将优惠券信息从缓存中删除
                    UserCouponDelayCloseEvent userCouponDelayCloseEvent = UserCouponDelayCloseEvent.builder()
                            .couponTemplateId(requestParam.getCouponTemplateId())
                            .userCouponId(String.valueOf(userCouponDO.getId()))
                            .userId(UserContext.getUserId())
                            .delayTime(validEndTime.getTime())
                            .build();
                    SendResult sendResult = couponDelayCloseProducer.sendMessage(userCouponDelayCloseEvent);

                    // 发送消息失败解决方案简单且高效的逻辑之一：打印日志并报警，通过日志搜集并重新投递
                    if (ObjectUtil.notEqual(sendResult.getSendStatus().name(), "SEND_OK")) {
                        log.warn("发送优惠券关闭延时队列失败，消息参数：{}", JSON.toJSONString(userCouponDelayCloseEvent));
                    }
                }
            } catch (Exception ex) {
                status.setRollbackOnly();
                // 优惠券已被领取完业务异常
                if (ex instanceof ServiceException) {
                    throw (ServiceException) ex;
                }
                if (ex instanceof DuplicateKeyException) {
                    log.error("用户重复领取优惠券，用户ID：{}，优惠券模板ID：{}", UserContext.getUserId(), requestParam.getCouponTemplateId());
                    throw new ServiceException("用户重复领取优惠券");
                }
                throw new ServiceException("优惠券领取异常，请稍候再试");
            }
        });
    }

    @Override
    public void redeemUserCouponByMQ(CouponTemplateRedeemReqDTO requestParam) {
        // 验证缓存是否存在，保障数据存在并且缓存中存在
        CouponTemplateQueryRespDTO couponTemplate = couponTemplateService.findCouponTemplate(BeanUtil.toBean(requestParam, CouponTemplateQueryReqDTO.class));

        // 验证领取的优惠券是否在活动有效时间
        boolean isInTime = DateUtil.isIn(new Date(), couponTemplate.getValidStartTime(), couponTemplate.getValidEndTime());
        if (!isInTime) {
            // 一般来说优惠券领取时间不到的时候，前端不会放开调用请求，可以理解这是用户调用接口在“攻击”
            throw new ClientException("不满足优惠券领取时间");
        }

        // 获取 LUA 脚本，并保存到 Hutool 的单例管理容器，下次直接获取不需要加载
        DefaultRedisScript<Long> buildLuaScript = Singleton.get(STOCK_DECREMENT_AND_SAVE_USER_RECEIVE_LUA_PATH, () -> {
            DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
            redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource(STOCK_DECREMENT_AND_SAVE_USER_RECEIVE_LUA_PATH)));
            redisScript.setResultType(Long.class);
            return redisScript;
        });

        // 验证用户是否符合优惠券领取条件
        JSONObject receiveRule = JSON.parseObject(couponTemplate.getReceiveRule());
        String limitPerPerson = receiveRule.getString("limitPerPerson");

        // 执行 LUA 脚本进行扣减库存以及增加 Redis 用户领券记录次数
        String couponTemplateCacheKey = String.format(EngineRedisConstant.COUPON_TEMPLATE_KEY, requestParam.getCouponTemplateId());
        String userCouponTemplateLimitCacheKey = String.format(EngineRedisConstant.USER_COUPON_TEMPLATE_LIMIT_KEY, UserContext.getUserId(), requestParam.getCouponTemplateId());
        Long stockDecrementLuaResult = stringRedisTemplate.execute(
                buildLuaScript,
                ListUtil.of(couponTemplateCacheKey, userCouponTemplateLimitCacheKey),
                String.valueOf(couponTemplate.getValidEndTime().getTime()), limitPerPerson
        );

        // 判断 LUA 脚本执行返回类，如果失败根据类型返回报错提示
        long firstField = StockDecrementReturnCombinedUtil.extractFirstField(stockDecrementLuaResult);
        if (RedisStockDecrementErrorEnum.isFail(firstField)) {
            throw new ServiceException(RedisStockDecrementErrorEnum.fromType(firstField));
        }

        UserCouponRedeemEvent userCouponRedeemEvent = UserCouponRedeemEvent.builder()
                .requestParam(requestParam)
                .receiveCount((int) StockDecrementReturnCombinedUtil.extractSecondField(stockDecrementLuaResult))
                .couponTemplate(couponTemplate)
                .userId(UserContext.getUserId())
                .build();
        SendResult sendResult = userCouponRedeemProducer.sendMessage(userCouponRedeemEvent);
        // 发送消息失败解决方案简单且高效的逻辑之一：打印日志并报警，通过日志搜集并重新投递
        if (ObjectUtil.notEqual(sendResult.getSendStatus().name(), "SEND_OK")) {
            log.warn("发送优惠券兑换消息失败，消息参数：{}", JSON.toJSONString(userCouponRedeemEvent));
        }
    }

    @Override
    public void createPaymentRecord(CouponCreatePaymentReqDTO requestParam) {
        //RLock lock = redissonClient.getLock(String.format(EngineRedisConstant.LOCK_COUPON_SETTLEMENT_KEY, requestParam.getCouponId()));
        //boolean tryLock = lock.tryLock();
        //if (!tryLock) {
        //    throw new ClientException("正在创建优惠券结算单，请稍候再试");
        //}

        //使用红锁
        // 创建一个红锁实例
        RLock lock1 = redissonClient.getLock(String.format(EngineRedisConstant.LOCK_COUPON_SETTLEMENT_KEY, requestParam.getCouponId()) + ":1");
        RLock lock2 = redissonClient.getLock(String.format(EngineRedisConstant.LOCK_COUPON_SETTLEMENT_KEY, requestParam.getCouponId()) + ":2");
        RLock lock2 = redissonClient.getLock(String.format(EngineRedisConstant.LOCK_COUPON_SETTLEMENT_KEY, requestParam.getCouponId()) + ":3");
        // ... 添加更多的锁实例，确保至少有3个独立的Redis实例
        RLock[] locks = {lock1, lock2 , lock3};

        // 尝试获取红锁
        boolean isLocked = redissonClient.getRedLock(locks).tryLock();
        if (!isLocked) {
            throw new ClientException("正在核销优惠券结算单，请稍候再试");
        }

        try {
            LambdaQueryWrapper<CouponSettlementDO> queryWrapper = Wrappers.lambdaQuery(CouponSettlementDO.class)
                    .eq(CouponSettlementDO::getCouponId, requestParam.getCouponId())
                    .eq(CouponSettlementDO::getUserId, Long.parseLong(UserContext.getUserId()))
                    .in(CouponSettlementDO::getStatus, 0, 2);

            // 验证优惠券是否正在使用或者已经被使用
            if (couponSettlementMapper.selectOne(queryWrapper) != null) {
                throw new ClientException("请检查优惠券是否已使用");
            }

            UserCouponDO userCouponDO = userCouponMapper.selectOne(Wrappers.lambdaQuery(UserCouponDO.class)
                    .eq(UserCouponDO::getId, requestParam.getCouponId())
                    .eq(UserCouponDO::getUserId, Long.parseLong(UserContext.getUserId())));

            // 验证用户优惠券状态和有效性
            if (Objects.isNull(userCouponDO)) {
                throw new ClientException("优惠券不存在");
            }
            if (userCouponDO.getValidEndTime().before(new Date())) {
                throw new ClientException("优惠券已过期");
            }
            if (userCouponDO.getStatus() != 0) {
                throw new ClientException("优惠券使用状态异常");
            }

            // 获取优惠券模板和消费规则
            CouponTemplateQueryRespDTO couponTemplate = couponTemplateService.findCouponTemplate(
                    new CouponTemplateQueryReqDTO(requestParam.getShopNumber(), String.valueOf(userCouponDO.getCouponTemplateId())));
            JSONObject consumeRule = JSONObject.parseObject(couponTemplate.getConsumeRule());

            // 计算折扣金额
            BigDecimal discountAmount;

            // 商品专属优惠券
            if (couponTemplate.getTarget().equals(0)) {
                // 获取第一个匹配的商品
                Optional<CouponCreatePaymentGoodsReqDTO> matchedGoods = requestParam.getGoodsList().stream()
                        .filter(each -> Objects.equals(couponTemplate.getGoods(), each.getGoodsNumber()))
                        .findFirst();

                if (matchedGoods.isEmpty()) {
                    throw new ClientException("商品信息与优惠券模板不符");
                }

                // 验证折扣金额
                CouponCreatePaymentGoodsReqDTO paymentGoods = matchedGoods.get();
                BigDecimal maximumDiscountAmount = consumeRule.getBigDecimal("maximumDiscountAmount");
                if (!paymentGoods.getGoodsAmount().subtract(maximumDiscountAmount).equals(paymentGoods.getGoodsPayableAmount())) {
                    throw new ClientException("商品折扣后金额异常");
                }

                discountAmount = maximumDiscountAmount;
            } else { // 店铺专属
                // 检查店铺编号（如果是店铺券）
                if (couponTemplate.getSource() == 0 && !requestParam.getShopNumber().equals(couponTemplate.getShopNumber())) {
                    throw new ClientException("店铺编号不一致");
                }

                BigDecimal termsOfUse = consumeRule.getBigDecimal("termsOfUse");
                if (requestParam.getOrderAmount().compareTo(termsOfUse) < 0) {
                    throw new ClientException("订单金额未满足使用条件");
                }

                BigDecimal maximumDiscountAmount = consumeRule.getBigDecimal("maximumDiscountAmount");

                switch (couponTemplate.getType()) {
                    case 0: // 立减券
                        discountAmount = maximumDiscountAmount;
                        break;
                    case 1: // 满减券
                        discountAmount = maximumDiscountAmount;
                        break;
                    case 2: // 折扣券
                        BigDecimal discountRate = consumeRule.getBigDecimal("discountRate");
                        discountAmount = requestParam.getOrderAmount().multiply(discountRate);
                        if (discountAmount.compareTo(maximumDiscountAmount) >= 0) {
                            discountAmount = maximumDiscountAmount;
                        }
                        break;
                    default:
                        throw new ClientException("无效的优惠券类型");
                }
            }

            // 计算折扣后金额并进行检查
            BigDecimal actualPayableAmount = requestParam.getOrderAmount().subtract(discountAmount);
            if (actualPayableAmount.compareTo(requestParam.getPayableAmount()) != 0) {
                throw new ClientException("折扣后金额不一致");
            }

            // 通过编程式事务减小事务范围
            transactionTemplate.executeWithoutResult(status -> {
                try {
                    // 创建优惠券结算单记录
                    CouponSettlementDO couponSettlementDO = CouponSettlementDO.builder()
                            .orderId(requestParam.getOrderId())
                            .couponId(requestParam.getCouponId())
                            .userId(Long.parseLong(UserContext.getUserId()))
                            .status(0)
                            .build();
                    couponSettlementMapper.insert(couponSettlementDO);

                    // 变更用户优惠券状态
                    LambdaUpdateWrapper<UserCouponDO> userCouponUpdateWrapper = Wrappers.lambdaUpdate(UserCouponDO.class)
                            .eq(UserCouponDO::getId, requestParam.getCouponId())
                            .eq(UserCouponDO::getUserId, Long.parseLong(UserContext.getUserId()))
                            .eq(UserCouponDO::getStatus, UserCouponStatusEnum.UNUSED.getCode());
                    UserCouponDO updateUserCouponDO = UserCouponDO.builder()
                            .status(UserCouponStatusEnum.LOCKING.getCode())
                            .build();
                    userCouponMapper.update(updateUserCouponDO, userCouponUpdateWrapper);
                } catch (Exception ex) {
                    log.error("创建优惠券结算单失败", ex);
                    status.setRollbackOnly();
                    throw ex;
                }
            });

            // 从用户可用优惠券列表中删除优惠券
            String userCouponItemCacheKey = StrUtil.builder()
                    .append(userCouponDO.getCouponTemplateId())
                    .append("_")
                    .append(userCouponDO.getId())
                    .toString();
            stringRedisTemplate.opsForZSet().remove(String.format(USER_COUPON_TEMPLATE_LIST_KEY, UserContext.getUserId()), userCouponItemCacheKey);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void processPayment(CouponProcessPaymentReqDTO requestParam) {
        //RLock lock = redissonClient.getLock(String.format(EngineRedisConstant.LOCK_COUPON_SETTLEMENT_KEY, requestParam.getCouponId()));
        //boolean tryLock = lock.tryLock();
        //if (!tryLock) {
        //    throw new ClientException("正在创建优惠券结算单，请稍候再试");
        //}

        //使用红锁
        // 创建一个红锁实例
        RLock lock1 = redissonClient.getLock(String.format(EngineRedisConstant.LOCK_COUPON_SETTLEMENT_KEY, requestParam.getCouponId()) + ":1");
        RLock lock2 = redissonClient.getLock(String.format(EngineRedisConstant.LOCK_COUPON_SETTLEMENT_KEY, requestParam.getCouponId()) + ":2");
        RLock lock2 = redissonClient.getLock(String.format(EngineRedisConstant.LOCK_COUPON_SETTLEMENT_KEY, requestParam.getCouponId()) + ":3");
        // ... 添加更多的锁实例，确保至少有3个独立的Redis实例
        RLock[] locks = {lock1, lock2 , lock3};

        // 尝试获取红锁
        boolean isLocked = redissonClient.getRedLock(locks).tryLock();
        if (!isLocked) {
            throw new ClientException("正在核销优惠券结算单，请稍候再试");
        }

        // 通过编程式事务减小事务范围
        transactionTemplate.executeWithoutResult(status -> {
            try {
                // 变更优惠券结算单状态为已支付
                LambdaUpdateWrapper<CouponSettlementDO> couponSettlementUpdateWrapper = Wrappers.lambdaUpdate(CouponSettlementDO.class)
                        .eq(CouponSettlementDO::getCouponId, requestParam.getCouponId())
                        .eq(CouponSettlementDO::getUserId, Long.parseLong(UserContext.getUserId()))
                        .eq(CouponSettlementDO::getStatus, 0);
                CouponSettlementDO couponSettlementDO = CouponSettlementDO.builder()
                        .status(2)
                        .build();
                int couponSettlementUpdated = couponSettlementMapper.update(couponSettlementDO, couponSettlementUpdateWrapper);
                if (!SqlHelper.retBool(couponSettlementUpdated)) {
                    log.error("核销优惠券结算单异常，请求参数：{}", com.alibaba.fastjson.JSON.toJSONString(requestParam));
                    throw new ServiceException("核销优惠券结算单异常");
                }

                // 变更用户优惠券状态
                LambdaUpdateWrapper<UserCouponDO> userCouponUpdateWrapper = Wrappers.lambdaUpdate(UserCouponDO.class)
                        .eq(UserCouponDO::getId, requestParam.getCouponId())
                        .eq(UserCouponDO::getUserId, Long.parseLong(UserContext.getUserId()))
                        .eq(UserCouponDO::getStatus, UserCouponStatusEnum.LOCKING.getCode());
                UserCouponDO userCouponDO = UserCouponDO.builder()
                        .status(UserCouponStatusEnum.USED.getCode())
                        .build();
                int userCouponUpdated = userCouponMapper.update(userCouponDO, userCouponUpdateWrapper);
                if (!SqlHelper.retBool(userCouponUpdated)) {
                    log.error("修改用户优惠券记录状态已使用异常，请求参数：{}", com.alibaba.fastjson.JSON.toJSONString(requestParam));
                    throw new ServiceException("修改用户优惠券记录状态异常");
                }
            } catch (Exception ex) {
                log.error("核销优惠券结算单失败", ex);
                status.setRollbackOnly();
                throw ex;
            } finally {
                lock.unlock();
            }
        });
    }

    @Override
    public void processRefund(CouponProcessRefundReqDTO requestParam) {
        //RLock lock = redissonClient.getLock(String.format(EngineRedisConstant.LOCK_COUPON_SETTLEMENT_KEY, requestParam.getCouponId()));
        //boolean tryLock = lock.tryLock();
        //if (!tryLock) {
        //    throw new ClientException("正在创建优惠券结算单，请稍候再试");
        //}

        //使用红锁
        // 创建一个红锁实例
        RLock lock1 = redissonClient.getLock(String.format(EngineRedisConstant.LOCK_COUPON_SETTLEMENT_KEY, requestParam.getCouponId()) + ":1");
        RLock lock2 = redissonClient.getLock(String.format(EngineRedisConstant.LOCK_COUPON_SETTLEMENT_KEY, requestParam.getCouponId()) + ":2");
        RLock lock2 = redissonClient.getLock(String.format(EngineRedisConstant.LOCK_COUPON_SETTLEMENT_KEY, requestParam.getCouponId()) + ":3");
        // ... 添加更多的锁实例，确保至少有3个独立的Redis实例
        RLock[] locks = {lock1, lock2 , lock3};

        // 尝试获取红锁
        boolean isLocked = redissonClient.getRedLock(locks).tryLock();
        if (!isLocked) {
            throw new ClientException("正在核销优惠券结算单，请稍候再试");
        }

        try {
            // 通过编程式事务减小事务范围
            transactionTemplate.executeWithoutResult(status -> {
                try {
                    // 变更优惠券结算单状态为已退款
                    LambdaUpdateWrapper<CouponSettlementDO> couponSettlementUpdateWrapper = Wrappers.lambdaUpdate(CouponSettlementDO.class)
                            .eq(CouponSettlementDO::getCouponId, requestParam.getCouponId())
                            .eq(CouponSettlementDO::getUserId, Long.parseLong(UserContext.getUserId()))
                            .eq(CouponSettlementDO::getStatus, 2);
                    CouponSettlementDO couponSettlementDO = CouponSettlementDO.builder()
                            .status(3)
                            .build();
                    int couponSettlementUpdated = couponSettlementMapper.update(couponSettlementDO, couponSettlementUpdateWrapper);
                    if (!SqlHelper.retBool(couponSettlementUpdated)) {
                        log.error("优惠券结算单退款异常，请求参数：{}", com.alibaba.fastjson.JSON.toJSONString(requestParam));
                        throw new ServiceException("核销优惠券结算单异常");
                    }

                    // 变更用户优惠券状态
                    LambdaUpdateWrapper<UserCouponDO> userCouponUpdateWrapper = Wrappers.lambdaUpdate(UserCouponDO.class)
                            .eq(UserCouponDO::getId, requestParam.getCouponId())
                            .eq(UserCouponDO::getUserId, Long.parseLong(UserContext.getUserId()))
                            .eq(UserCouponDO::getStatus, UserCouponStatusEnum.USED.getCode());
                    UserCouponDO userCouponDO = UserCouponDO.builder()
                            .status(UserCouponStatusEnum.UNUSED.getCode())
                            .build();
                    int userCouponUpdated = userCouponMapper.update(userCouponDO, userCouponUpdateWrapper);
                    if (!SqlHelper.retBool(userCouponUpdated)) {
                        log.error("修改用户优惠券记录状态未使用异常，请求参数：{}", com.alibaba.fastjson.JSON.toJSONString(requestParam));
                        throw new ServiceException("修改用户优惠券记录状态异常");
                    }
                } catch (Exception ex) {
                    log.error("执行优惠券结算单退款失败", ex);
                    status.setRollbackOnly();
                    throw ex;
                }
            });

            // 查询出来优惠券再放回缓存
            UserCouponDO userCouponDO = userCouponMapper.selectOne(Wrappers.lambdaQuery(UserCouponDO.class)
                    .eq(UserCouponDO::getUserId, Long.parseLong(UserContext.getUserId()))
                    .eq(UserCouponDO::getId, requestParam.getCouponId())
            );
            String userCouponItemCacheKey = StrUtil.builder()
                    .append(userCouponDO.getCouponTemplateId())
                    .append("_")
                    .append(userCouponDO.getId())
                    .toString();
            stringRedisTemplate.opsForZSet().add(String.format(USER_COUPON_TEMPLATE_LIST_KEY, UserContext.getUserId()), userCouponItemCacheKey, userCouponDO.getReceiveTime().getTime());
        } finally {
            lock.unlock();
        }
    }
}
