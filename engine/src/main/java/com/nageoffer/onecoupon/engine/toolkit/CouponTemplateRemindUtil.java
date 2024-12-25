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

package com.nageoffer.onecoupon.engine.toolkit;

import cn.hutool.core.date.DateUtil;
import com.nageoffer.onecoupon.engine.common.enums.CouponRemindTypeEnum;
import com.nageoffer.onecoupon.engine.dto.resp.CouponTemplateRemindQueryRespDTO;
import com.nageoffer.onecoupon.framework.exception.ClientException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 优惠券预约提醒工具类
 * <p>
 * 作者：优雅
 * 加项目群：早加入就是优势！500人内部项目群，分享的知识总有你需要的 <a href="https://t.zsxq.com/cw7b9" />
 * 开发时间：2024-07-20
 */
public class CouponTemplateRemindUtil {

    /**
     * 下一个类型的位移量，每个类型占用12个bit位，共计60分钟
     */
    private static final int NEXT_TYPE_BITS = 12;

    /**
     * 5分钟为一个间隔
     */
    private static final int TIME_INTERVAL = 5;

    /**
     * 提醒方式的数量
     */
    private static final int TYPE_COUNT = CouponRemindTypeEnum.values().length;

    /**
     * 填充预约信息
     */
    public static void fillRemindInformation(CouponTemplateRemindQueryRespDTO resp, Long information) {
        List<Date> dateList = new ArrayList<>();
        List<String> remindType = new ArrayList<>();
        Date validStartTime = resp.getValidStartTime();
        for (int i = NEXT_TYPE_BITS - 1; i >= 0; i--) {
            // 按时间节点倒叙遍历，即离开抢时间最久，离现在最近
            for (int j = 0; j < TYPE_COUNT; j++) {
                // 对于每个时间节点，遍历所有类型
                if (((information >> (j * NEXT_TYPE_BITS + i)) & 1) == 1) {
                    // 该时间节点的该提醒类型用户有预约
                    Date date = DateUtil.offsetMinute(validStartTime, -((i + 1) * TIME_INTERVAL));
                    dateList.add(date);
                    remindType.add(CouponRemindTypeEnum.getDescribeByType(j));
                }
            }
        }
        resp.setRemindTime(dateList);
        resp.setRemindType(remindType);
    }

    /**
     * 根据预约时间和预约类型计算bitmap
     */
    public static Long calculateBitMap(Integer remindTime, Integer type) {
        if (remindTime > TIME_INTERVAL * NEXT_TYPE_BITS) {
            throw new ClientException("预约提醒的时间不能早于开票前" + TIME_INTERVAL * NEXT_TYPE_BITS + "分钟");
        }
        return 1L << (type * NEXT_TYPE_BITS + Math.max(0, remindTime / TIME_INTERVAL - 1));
    }
}
