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

package com.nageoffer.onecoupon.settlement.service;

import com.nageoffer.onecoupon.settlement.toolkit.CouponFactory;
import com.nageoffer.onecoupon.settlement.dao.entity.CouponTemplateDO;
import com.nageoffer.onecoupon.settlement.dao.entity.DiscountCouponDO;
import com.nageoffer.onecoupon.settlement.dao.entity.FixedDiscountCouponDO;
import com.nageoffer.onecoupon.settlement.dao.entity.ThresholdCouponDO;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CouponFactoryTests：用于测试 CouponFactory 工厂类中优惠券创建逻辑
 */
public class CouponFactoryTests {

    @Test
    public void testCreateFixedDiscountCoupon() {
        CouponTemplateDO baseCoupon = createBaseCoupon(0);
        Map<String, Object> params = new HashMap<>();
        params.put("discountAmount", 50);

        FixedDiscountCouponDO fixedDiscountCoupon = (FixedDiscountCouponDO) CouponFactory.createCoupon(baseCoupon, params);

        assertNotNull(fixedDiscountCoupon);
        assertEquals(50, fixedDiscountCoupon.getDiscountAmount());
        assertEquals(baseCoupon.getId(), fixedDiscountCoupon.getId());
        assertEquals(baseCoupon.getShopNumber(), fixedDiscountCoupon.getShopNumber());
    }

    @Test
    public void testCreateThresholdDiscountCoupon() {
        CouponTemplateDO baseCoupon = createBaseCoupon(1);
        Map<String, Object> params = new HashMap<>();
        params.put("thresholdAmount", 100);
        params.put("discountAmount", 20);

        ThresholdCouponDO thresholdDiscountCoupon = (ThresholdCouponDO) CouponFactory.createCoupon(baseCoupon, params);

        assertNotNull(thresholdDiscountCoupon);
        assertEquals(100, thresholdDiscountCoupon.getThresholdAmount());
        assertEquals(20, thresholdDiscountCoupon.getDiscountAmount());
        assertEquals(baseCoupon.getId(), thresholdDiscountCoupon.getId());
        assertEquals(baseCoupon.getShopNumber(), thresholdDiscountCoupon.getShopNumber());
    }

    @Test
    public void testCreateDiscountCoupon() {
        CouponTemplateDO baseCoupon = createBaseCoupon(2);
        Map<String, Object> params = new HashMap<>();
        params.put("discountRate", 0.8);

        DiscountCouponDO discountCoupon = (DiscountCouponDO) CouponFactory.createCoupon(baseCoupon, params);

        assertNotNull(discountCoupon);
        assertEquals(0.8, discountCoupon.getDiscountRate());
        assertEquals(baseCoupon.getId(), discountCoupon.getId());
        assertEquals(baseCoupon.getShopNumber(), discountCoupon.getShopNumber());
    }

    @Test
    public void testInvalidCouponType() {
        CouponTemplateDO baseCoupon = createBaseCoupon(99); // Invalid type
        Map<String, Object> params = new HashMap<>();

        assertThrows(IllegalArgumentException.class, () -> {
            CouponFactory.createCoupon(baseCoupon, params);
        });
    }

    private CouponTemplateDO createBaseCoupon(int type) {
        return CouponTemplateDO.builder()
                .id(1L)
                .shopNumber(101L)
                .name("Test Coupon")
                .source(0)
                .target(0)
                .goods("Test Goods")
                .type(type)
                .validStartTime(new Date())
                .validEndTime(new Date(System.currentTimeMillis() + 86400000L)) // 1 day later
                .stock(100)
                .receiveRule("{}")
                .consumeRule("{}")
                .status(0)
                .createTime(new Date())
                .updateTime(new Date())
                .delFlag(0)
                .build();
    }
}
