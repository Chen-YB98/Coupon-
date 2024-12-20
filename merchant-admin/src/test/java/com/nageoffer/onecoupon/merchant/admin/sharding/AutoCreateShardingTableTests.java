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

package com.nageoffer.onecoupon.merchant.admin.sharding;

import org.junit.jupiter.api.Test;

/**
 * 创建优惠券项目中需要分片数据库表 SQL 语句
 * <p>
 * 作者：马丁
 * 加项目群：早加入就是优势！500人内部项目群，分享的知识总有你需要的 <a href="https://t.zsxq.com/cw7b9" />
 * 开发时间：2024-07-10
 */
public class AutoCreateShardingTableTests {

    private final String couponTable = "CREATE TABLE `t_coupon_template_%d` (\n" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',\n" +
            "  `name` varchar(256) DEFAULT NULL COMMENT '优惠券名称',\n" +
            "  `shop_number` bigint(20) DEFAULT NULL COMMENT '店铺编号',\n" +
            "  `source` tinyint(1) DEFAULT NULL COMMENT '优惠券来源 0：店铺券 1：平台券',\n" +
            "  `target` tinyint(1) DEFAULT NULL COMMENT '优惠对象 0：商品专属 1：全店通用',\n" +
            "  `goods` text COMMENT '优惠商品编码',\n" +
            "  `type` tinyint(1) DEFAULT NULL COMMENT '优惠类型 0：立减券 1：满减券 2：折扣券',\n" +
            "  `valid_start_time` datetime DEFAULT NULL COMMENT '有效期开始时间',\n" +
            "  `valid_end_time` datetime DEFAULT NULL COMMENT '有效期结束时间',\n" +
            "  `stock` int(11) DEFAULT NULL COMMENT '库存',\n" +
            "  `receive_rule` json DEFAULT NULL COMMENT '领取规则',\n" +
            "  `consume_rule` json DEFAULT NULL COMMENT '消耗规则',\n" +
            "  `status` tinyint(1) DEFAULT NULL COMMENT '优惠券状态 0：生效中 1：已结束',\n" +
            "  `create_time` datetime DEFAULT NULL COMMENT '创建时间',\n" +
            "  `update_time` datetime DEFAULT NULL COMMENT '修改时间',\n" +
            "  `del_flag` tinyint(1) DEFAULT NULL COMMENT '删除标识 0：未删除 1：已删除',\n" +
            "  PRIMARY KEY (`id`),\n" +
            "  KEY `idx_shop_number` (`shop_number`) USING BTREE\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='优惠券模板表';";

    private final String couponLogTable = "CREATE TABLE `t_coupon_template_log_%d` (\n" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',\n" +
            "  `shop_number` bigint(20) DEFAULT NULL COMMENT '店铺编号',\n" +
            "  `coupon_template_id` bigint(20) DEFAULT NULL COMMENT '优惠券模板ID',\n" +
            "  `operator_id` bigint(20) DEFAULT NULL COMMENT '操作人',\n" +
            "  `operation_log` text COMMENT '操作日志',\n" +
            "  `original_data` varchar(1024) DEFAULT NULL COMMENT '原始数据',\n" +
            "  `modified_data` varchar(1024) DEFAULT NULL COMMENT '修改后数据',\n" +
            "  `create_time` datetime DEFAULT NULL COMMENT '创建时间',\n" +
            "  PRIMARY KEY (`id`),\n" +
            "  KEY `idx_shop_number` (`shop_number`) USING BTREE\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='优惠券模板操作日志表';";

    @Test
    public void autoCreateCouponTemplateShardingTable() {
        for (int i = 0; i < 16; i++) {
            System.out.println(String.format(couponTable, i));
        }
    }

    @Test
    public void autoCreateCouponTemplateLogShardingTable() {
        for (int i = 0; i < 16; i++) {
            System.out.println(String.format(couponLogTable, i));
        }
    }
}
