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

package com.nageoffer.onecoupon.search.mq.consumer;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import com.nageoffer.onecoupon.search.common.constant.SearchRockerMQConstant;
import com.nageoffer.onecoupon.search.common.enums.CouponTemplateStatusEnum;
import com.nageoffer.onecoupon.search.dao.entity.CouponTemplateDO;
import com.nageoffer.onecoupon.search.dao.entity.CouponTemplateDoc;
import com.nageoffer.onecoupon.search.mq.event.CanalBinlogEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 通过 Canal 监听优惠券模板表 Binlog 投递消息队列消费
 * <p>
 * 作者：蛋仔
 * 加项目群：早加入就是优势！500人内部项目群，分享的知识总有你需要的 <a href="https://t.zsxq.com/cw7b9" />
 * 开发时间：2024-07-31
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = SearchRockerMQConstant.TEMPLATE_COUPON_BINLOG_SYNC_TOPIC_KEY,
        consumerGroup = SearchRockerMQConstant.TEMPLATE_COUPON_BINLOG_SYNC_CG_KEY
)
public class CanalBinlogSyncCouponTemplateConsumer implements RocketMQListener<CanalBinlogEvent> {

    private final ElasticsearchTemplate elasticsearchTemplate;

    @Override
    public void onMessage(CanalBinlogEvent canalBinlogEvent) {
        log.info("{}", canalBinlogEvent);
        Map<String, Object> first = CollUtil.getFirst(canalBinlogEvent.getData());
        CouponTemplateDO couponTemplate = BeanUtil.toBean(first, CouponTemplateDO.class);
        CouponTemplateDoc couponTemplateDoc = BeanUtil.copyProperties(couponTemplate, CouponTemplateDoc.class);
        switch (canalBinlogEvent.getType()) {
            // 新增优惠券
            case "INSERT": {
                elasticsearchTemplate.save(couponTemplateDoc);
                break;
            }
            // 更新优惠券
            case "UPDATE": {
                // 优惠券已结束或已删除, 都将该数据从ES中移除, 否则进行更新操作
                if (CouponTemplateStatusEnum.EXPIRED.getCode().equals(couponTemplate.getStatus())
                        || CouponTemplateStatusEnum.DELETED.getCode().equals(couponTemplate.getDelFlag())) {
                    elasticsearchTemplate.delete(couponTemplateDoc);
                    break;
                }
                boolean exists = elasticsearchTemplate.exists(couponTemplateDoc.getId().toString(), CouponTemplateDoc.class);
                // 当更新主体不存在时，则转换成新增
                if (!exists) {
                    elasticsearchTemplate.save(couponTemplateDoc);
                    break;
                }
                elasticsearchTemplate.update(couponTemplateDoc);
                break;
            }
            // 硬删除优惠券, 一般业务上不会走该分支, 为了避免直接操作数据库导致数据不一致, 因此也加上该段逻辑
            case "DELETE": {
                elasticsearchTemplate.delete(couponTemplateDoc);
                break;
            }
            default:
        }
    }
}
