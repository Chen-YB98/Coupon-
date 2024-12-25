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

package com.nageoffer.onecoupon.engine.mq.consumer;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.extension.toolkit.SqlHelper;
import com.nageoffer.onecoupon.engine.common.constant.EngineRedisConstant;
import com.nageoffer.onecoupon.engine.common.constant.EngineRockerMQConstant;
import com.nageoffer.onecoupon.engine.common.context.UserContext;
import com.nageoffer.onecoupon.engine.common.enums.UserCouponStatusEnum;
import com.nageoffer.onecoupon.engine.dao.entity.UserCouponDO;
import com.nageoffer.onecoupon.engine.dao.mapper.CouponTemplateMapper;
import com.nageoffer.onecoupon.engine.dao.mapper.UserCouponMapper;
import com.nageoffer.onecoupon.engine.dto.req.CouponTemplateRedeemReqDTO;
import com.nageoffer.onecoupon.engine.dto.resp.CouponTemplateQueryRespDTO;
import com.nageoffer.onecoupon.engine.mq.base.MessageWrapper;
import com.nageoffer.onecoupon.engine.mq.event.UserCouponDelayCloseEvent;
import com.nageoffer.onecoupon.engine.mq.event.UserCouponRedeemEvent;
import com.nageoffer.onecoupon.engine.mq.producer.UserCouponDelayCloseProducer;
import com.nageoffer.onecoupon.framework.idempotent.NoMQDuplicateConsume;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;

/**
 * 用户兑换优惠券消息消费者
 * <p>
 * 作者：马丁
 * 加项目群：早加入就是优势！500人内部沟通群，分享的知识总有你需要的 <a href="https://t.zsxq.com/cw7b9" />
 * 开发时间：2024-09-10
 */
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = EngineRockerMQConstant.COUPON_TEMPLATE_REDEEM_TOPIC_KEY,
        consumerGroup = EngineRockerMQConstant.COUPON_TEMPLATE_REDEEM_CG_KEY
)
@Slf4j(topic = "UserCouponRedeemConsumer")
public class UserCouponRedeemConsumer implements RocketMQListener<MessageWrapper<UserCouponRedeemEvent>> {

    private final UserCouponMapper userCouponMapper;
    private final CouponTemplateMapper couponTemplateMapper;
    private final UserCouponDelayCloseProducer couponDelayCloseProducer;
    private final StringRedisTemplate stringRedisTemplate;

    @NoMQDuplicateConsume(
            keyPrefix = "user-coupon-redeem:",
            key = "#messageWrapper.keys",
            keyTimeout = 600
    )
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void onMessage(MessageWrapper<UserCouponRedeemEvent> messageWrapper) {
        // 开头打印日志，平常可 Debug 看任务参数，线上可报平安（比如消息是否消费，重新投递时获取参数等）
        log.info("[消费者] 用户兑换优惠券 - 执行消费逻辑，消息体：{}", JSON.toJSONString(messageWrapper));

        CouponTemplateRedeemReqDTO requestParam = messageWrapper.getMessage().getRequestParam();
        CouponTemplateQueryRespDTO couponTemplate = messageWrapper.getMessage().getCouponTemplate();
        String userId = messageWrapper.getMessage().getUserId();

        int decremented = couponTemplateMapper.decrementCouponTemplateStock(Long.parseLong(requestParam.getShopNumber()), Long.parseLong(requestParam.getCouponTemplateId()), 1L);
        if (!SqlHelper.retBool(decremented)) {
            log.warn("[消费者] 用户兑换优惠券 - 执行消费逻辑，扣减优惠券数据库库存失败，消息体：{}", JSON.toJSONString(messageWrapper));
            return;
        }

        // 添加 Redis 用户领取的优惠券记录列表
        Date now = new Date();
        DateTime validEndTime = DateUtil.offsetHour(now, JSON.parseObject(couponTemplate.getConsumeRule()).getInteger("validityPeriod"));
        UserCouponDO userCouponDO = UserCouponDO.builder()
                .couponTemplateId(Long.parseLong(requestParam.getCouponTemplateId()))
                .userId(Long.parseLong(userId))
                .source(requestParam.getSource())
                .receiveCount(messageWrapper.getMessage().getReceiveCount())
                .status(UserCouponStatusEnum.UNUSED.getCode())
                .receiveTime(now)
                .validStartTime(now)
                .validEndTime(validEndTime)
                .build();
        userCouponMapper.insert(userCouponDO);

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
            log.warn("[消费者] 用户兑换优惠券 - 执行消费逻辑，查询Redis用户优惠券记录为空或抛异常，可能Redis宕机或主从复制数据丢失，基础错误信息：{}", ex.getMessage());
            // 如果直接抛异常大概率 Redis 宕机了，所以应该写个延时队列向 Redis 重试放入值。为了避免代码复杂性，这里直接写新增，大家知道最优解决方案即可
            stringRedisTemplate.opsForZSet().add(userCouponListCacheKey, userCouponItemCacheKey, now.getTime());
        }

        // 发送延时消息队列，等待优惠券到期后，将优惠券信息从缓存中删除
        UserCouponDelayCloseEvent userCouponDelayCloseEvent = UserCouponDelayCloseEvent.builder()
                .couponTemplateId(requestParam.getCouponTemplateId())
                .userCouponId(String.valueOf(userCouponDO.getId()))
                .userId(userId)
                .delayTime(validEndTime.getTime())
                .build();
        SendResult sendResult = couponDelayCloseProducer.sendMessage(userCouponDelayCloseEvent);

        // 发送消息失败解决方案简单且高效的逻辑之一：打印日志并报警，通过日志搜集并重新投递
        if (ObjectUtil.notEqual(sendResult.getSendStatus().name(), "SEND_OK")) {
            log.warn("[消费者] 用户兑换优惠券 - 执行消费逻辑，发送优惠券关闭延时队列失败，消息参数：{}", JSON.toJSONString(userCouponDelayCloseEvent));
        }
    }
}
