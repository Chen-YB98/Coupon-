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

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.nageoffer.onecoupon.engine.common.constant.EngineRedisConstant;
import com.nageoffer.onecoupon.engine.common.constant.EngineRockerMQConstant;
import com.nageoffer.onecoupon.engine.common.context.UserContext;
import com.nageoffer.onecoupon.engine.mq.event.CanalBinlogEvent;
import com.nageoffer.onecoupon.engine.mq.event.UserCouponDelayCloseEvent;
import com.nageoffer.onecoupon.engine.mq.producer.UserCouponDelayCloseProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Map;

/**
 * 通过 Canal 监听用户优惠券表 Binlog 投递消息队列消费
 * <p>
 * 作者：马丁
 * 加项目群：早加入就是优势！500人内部沟通群，分享的知识总有你需要的 <a href="https://t.zsxq.com/cw7b9" />
 * 开发时间：2024-07-25
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = EngineRockerMQConstant.USER_COUPON_BINLOG_SYNC_TOPIC_KEY,
        consumerGroup = EngineRockerMQConstant.USER_COUPON_BINLOG_SYNC_CG_KEY
)
public class CanalBinlogSyncUserCouponConsumer implements RocketMQListener<CanalBinlogEvent> {

    private final StringRedisTemplate stringRedisTemplate;
    private final UserCouponDelayCloseProducer couponDelayCloseProducer;

    @Value("${one-coupon.user-coupon-list.save-cache.type:direct}")
    private String userCouponListSaveCacheType;

    @Override
    public void onMessage(CanalBinlogEvent canalBinlogEvent) {
        if (ObjectUtil.notEqual(userCouponListSaveCacheType, "binlog")) {
            return;
        }
        Map<String, Object> first = CollUtil.getFirst(canalBinlogEvent.getData());
        String couponTemplateId = first.get("coupon_template_id").toString();
        String userCouponId = first.get("id").toString();
        // 用户优惠券创建事件
        if (ObjectUtil.equal(canalBinlogEvent.getType(), "INSERT")) {
            // 添加用户领取优惠券模板缓存记录
            String userCouponListCacheKey = String.format(EngineRedisConstant.USER_COUPON_TEMPLATE_LIST_KEY, UserContext.getUserId());
            String userCouponItemCacheKey = StrUtil.builder()
                    .append(couponTemplateId)
                    .append("_")
                    .append(userCouponId)
                    .toString();
            Date receiveTime = DateUtil.parse(first.get("receive_time").toString());
            stringRedisTemplate.opsForZSet().add(userCouponListCacheKey, userCouponItemCacheKey, receiveTime.getTime());

            // 由于 Redis 在持久化或主从复制的极端情况下可能会出现数据丢失，而我们对指令丢失几乎无法容忍，因此我们采用经典的写后查询策略来应对这一问题
            Double scored;
            try {
                scored = stringRedisTemplate.opsForZSet().score(userCouponListCacheKey, userCouponItemCacheKey);
                // scored 为空意味着可能 Redis Cluster 主从同步丢失了数据，比如 Redis 主节点还没有同步到从节点就宕机了，解决方案就是再新增一次
                if (scored == null) {
                    // 如果这里也新增失败了怎么办？我们大概率做不到绝对的万无一失，只能尽可能增加成功率
                    stringRedisTemplate.opsForZSet().add(userCouponListCacheKey, userCouponItemCacheKey, receiveTime.getTime());
                }
            } catch (Throwable ex) {
                log.warn("查询Redis用户优惠券记录为空或抛异常，可能Redis宕机或主从复制数据丢失，基础错误信息：{}", ex.getMessage());
                // 如果直接抛异常大概率 Redis 宕机了，所以应该写个延时队列向 Redis 重试放入值。为了避免代码复杂性，这里直接写新增，大家知道最优解决方案即可
                stringRedisTemplate.opsForZSet().add(userCouponListCacheKey, userCouponItemCacheKey, receiveTime.getTime());
            }

            // 发送延时消息队列，等待优惠券到期后，将优惠券信息从缓存中删除
            UserCouponDelayCloseEvent userCouponDelayCloseEvent = UserCouponDelayCloseEvent.builder()
                    .couponTemplateId(couponTemplateId)
                    .userCouponId(userCouponId)
                    .userId(UserContext.getUserId())
                    .delayTime(DateUtil.parse(first.get("valid_end_time").toString()).getTime())
                    .build();
            SendResult sendResult = couponDelayCloseProducer.sendMessage(userCouponDelayCloseEvent);

            // 发送消息失败解决方案简单且高效的逻辑之一：打印日志并报警，通过日志搜集并重新投递
            if (ObjectUtil.notEqual(sendResult.getSendStatus().name(), "SEND_OK")) {
                log.warn("发送优惠券关闭延时队列失败，消息参数：{}", JSON.toJSONString(userCouponDelayCloseEvent));
            }
        }
    }
}
