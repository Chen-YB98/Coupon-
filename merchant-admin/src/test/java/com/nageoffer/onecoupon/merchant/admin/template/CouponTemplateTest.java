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

package com.nageoffer.onecoupon.merchant.admin.template;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Assert;
import com.alibaba.fastjson2.JSONObject;
import com.nageoffer.onecoupon.merchant.admin.common.enums.CouponTemplateStatusEnum;
import com.nageoffer.onecoupon.merchant.admin.dao.entity.CouponTemplateDO;
import com.nageoffer.onecoupon.merchant.admin.service.CouponTemplateService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.Date;

@SpringBootTest
public class CouponTemplateTest {

    @Autowired
    private CouponTemplateService couponTemplateService;

    public CouponTemplateDO buildCouponTemplateDO(BigDecimal termsOfUse, BigDecimal maximumDiscountAmount, Integer target, Integer type, String goods) {
        JSONObject receiveRule = new JSONObject();
        receiveRule.put("limitPerPerson", 1); // 每人限领
        receiveRule.put("usageInstructions", "3"); // 使用说明
        JSONObject consumeRule = new JSONObject();
        consumeRule.put("termsOfUse", termsOfUse); // 使用条件 满 x 元可用
        consumeRule.put("maximumDiscountAmount", maximumDiscountAmount); // 最大优惠金额
        consumeRule.put("explanationOfUnmetConditions", "3"); // 不满足使用条件说明
        if (type == 2) {
            consumeRule.put("discountRate", "0.6"); // 折扣券专属，折扣率
        }
        consumeRule.put("validityPeriod", 48); // 自领取优惠券后有效时间，单位小时
        CouponTemplateDO couponTemplateDO = CouponTemplateDO.builder()
                .shopNumber(1810714735922956666L) // 店铺编号
                .name("商品立减券") // 优惠券名称
                .source(0) // 优惠券来源 0：店铺券 1：平台券
                .target(target) // 优惠对象 0：商品专属 1：全店通用
                .type(type) // 优惠类型 0：立减券 1：满减券 2：折扣券
                .goods(goods) // 优惠商品编码
                .validStartTime(new Date()) // 有效期开始时间
                .validEndTime(DateUtil.offsetMonth(new Date(), 6)) // 有效期结束时间
                .stock(10) // 库存
                .receiveRule(receiveRule.toString()) // 领取规则
                .consumeRule(consumeRule.toString()) // 消耗规则
                .status(CouponTemplateStatusEnum.ACTIVE.getStatus()) // 优惠券状态 0：生效中 1：已结束
                .build();
        return couponTemplateDO;
    }

    /**
     * 测试新增优惠券模板方法
     */
    @Test
    public void testInsertCouponTemplate() {
        CouponTemplateDO couponTemplateDO = buildCouponTemplateDO(new BigDecimal("10"), new BigDecimal("3"), 1, 0, null);
        boolean saved = couponTemplateService.save(couponTemplateDO);
        Assert.isTrue(saved);
    }
}
