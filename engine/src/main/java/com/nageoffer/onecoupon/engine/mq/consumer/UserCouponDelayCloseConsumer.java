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

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.nageoffer.onecoupon.engine.common.constant.EngineRedisConstant;
import com.nageoffer.onecoupon.engine.common.constant.EngineRockerMQConstant;
import com.nageoffer.onecoupon.engine.common.context.UserContext;
import com.nageoffer.onecoupon.engine.common.enums.UserCouponStatusEnum;
import com.nageoffer.onecoupon.engine.dao.entity.UserCouponDO;
import com.nageoffer.onecoupon.engine.dao.mapper.UserCouponMapper;
import com.nageoffer.onecoupon.engine.mq.base.MessageWrapper;
import com.nageoffer.onecoupon.engine.mq.event.UserCouponDelayCloseEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * 用户优惠券延时关闭消费者
 * <p>
 * 作者：马丁
 * 加项目群：早加入就是优势！500人内部项目群，分享的知识总有你需要的 <a href="https://t.zsxq.com/cw7b9" />
 * 开发时间：2024-07-18
 */
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = EngineRockerMQConstant.USER_COUPON_DELAY_CLOSE_TOPIC_KEY,
        consumerGroup = EngineRockerMQConstant.USER_COUPON_DELAY_CLOSE_CG_KEY
)
@Slf4j(topic = "UserCouponDelayCloseConsumer")
public class UserCouponDelayCloseConsumer implements RocketMQListener<MessageWrapper<UserCouponDelayCloseEvent>> {

    private final StringRedisTemplate stringRedisTemplate;
    private final UserCouponMapper userCouponMapper;

    @Override
    public void onMessage(MessageWrapper<UserCouponDelayCloseEvent> messageWrapper) {
        // 开头打印日志，平常可 Debug 看任务参数，线上可报平安（比如消息是否消费，重新投递时获取参数等）
        log.info("[消费者] 延迟关闭用户已领取优惠券 - 执行消费逻辑，消息体：{}", JSON.toJSONString(messageWrapper));
        UserCouponDelayCloseEvent event = messageWrapper.getMessage();

        // 删除用户领取优惠券模板缓存记录
        String userCouponListCacheKey = String.format(EngineRedisConstant.USER_COUPON_TEMPLATE_LIST_KEY, UserContext.getUserId());
        String userCouponItemCacheKey = StrUtil.builder()
                .append(event.getCouponTemplateId())
                .append("_")
                .append(event.getUserCouponId())
                .toString();
        Long removed = stringRedisTemplate.opsForZSet().remove(userCouponListCacheKey, userCouponItemCacheKey);
        if (removed == null || removed == 0L) {
            return;
        }

        // 修改用户领券记录状态为已过期
        UserCouponDO userCouponDO = UserCouponDO.builder()
                .status(UserCouponStatusEnum.EXPIRED.getCode())
                .build();
        LambdaUpdateWrapper<UserCouponDO> updateWrapper = Wrappers.lambdaUpdate(UserCouponDO.class)
                .eq(UserCouponDO::getId, event.getUserCouponId())
                .eq(UserCouponDO::getUserId, event.getUserId())
                .eq(UserCouponDO::getStatus, UserCouponStatusEnum.UNUSED.getCode())
                .eq(UserCouponDO::getCouponTemplateId, event.getCouponTemplateId());
        userCouponMapper.update(userCouponDO, updateWrapper);
    }
}
