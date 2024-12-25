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

package com.nageoffer.onecoupon.settlement.toolkit;

import com.nageoffer.onecoupon.settlement.common.enums.DiscountTypeEnum;
import com.nageoffer.onecoupon.settlement.dao.entity.CouponTemplateDO;
import com.nageoffer.onecoupon.settlement.dao.entity.DiscountCouponDO;
import com.nageoffer.onecoupon.settlement.dao.entity.FixedDiscountCouponDO;
import com.nageoffer.onecoupon.settlement.dao.entity.ThresholdCouponDO;
import com.nageoffer.onecoupon.settlement.service.strategy.CouponCalculationStrategy;
import com.nageoffer.onecoupon.settlement.service.strategy.DiscountCalculationStrategy;
import com.nageoffer.onecoupon.settlement.service.strategy.FixedDiscountCalculationStrategy;
import com.nageoffer.onecoupon.settlement.service.strategy.ThresholdCalculationStrategy;

import java.util.Map;

/**
 * 优惠券工厂类，用于创建不同类型的优惠券实例
 * <p>
 * 作者：Henry Wan
 * 加项目群：早加入就是优势！500人内部项目群，分享的知识总有你需要的 <a href="https://t.zsxq.com/cw7b9" />
 * 开发时间：2024-07-24
 */
public class CouponFactory {

    /**
     * 创建优惠券对象，根据传入的 CouponTemplateDO 对象和附加参数生成具体的优惠券实例。
     *
     * @param coupon           基础优惠券模板对象
     * @param additionalParams 附加参数，包含优惠券类型所需的额外信息
     * @return 具体的优惠券实例
     */
    public static CouponTemplateDO createCoupon(CouponTemplateDO coupon, Map<String, Object> additionalParams) {
        // 检查优惠券类型是否有效
        if (coupon.getType() == null || coupon.getType() >= DiscountTypeEnum.values().length || coupon.getType() < 0) {
            throw new IllegalArgumentException("Invalid coupon type");
        }

        // 根据优惠券类型创建具体的优惠券实例
        switch (DiscountTypeEnum.values()[coupon.getType()]) {
            case FIXED_DISCOUNT:
                // 固定折扣类型优惠券
                Integer fixedDiscountAmount = (Integer) additionalParams.get("discountAmount");
                return new FixedDiscountCouponDO(coupon, fixedDiscountAmount);

            case THRESHOLD_DISCOUNT:
                // 阈值折扣类型优惠券
                Integer thresholdAmount = (Integer) additionalParams.get("thresholdAmount");
                Integer thresholdDiscountAmount = (Integer) additionalParams.get("discountAmount");
                return new ThresholdCouponDO(coupon, thresholdAmount, thresholdDiscountAmount);

            case DISCOUNT_COUPON:
                // 折扣券类型优惠券
                Double discountRate = (Double) additionalParams.get("discountRate");
                return new DiscountCouponDO(coupon, discountRate);

            default:
                // 如果类型无效，抛出异常
                throw new IllegalArgumentException("Invalid coupon type");
        }
    }

    /**
     * 获取优惠券计算策略。
     *
     * @param coupon 基础优惠券模板对象
     * @return 对应的优惠券计算策略
     */
    public static CouponCalculationStrategy getCouponCalculationStrategy(CouponTemplateDO coupon) {
        switch (DiscountTypeEnum.values()[coupon.getType()]) {
            case FIXED_DISCOUNT:
                return new FixedDiscountCalculationStrategy();
            case THRESHOLD_DISCOUNT:
                return new ThresholdCalculationStrategy();
            case DISCOUNT_COUPON:
                return new DiscountCalculationStrategy();
            default:
                throw new IllegalArgumentException("Invalid coupon type");
        }
    }
}