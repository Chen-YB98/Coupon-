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

package com.nageoffer.onecoupon.merchant.admin.job;

import cn.hutool.core.collection.CollUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.nageoffer.onecoupon.framework.result.Result;
import com.nageoffer.onecoupon.framework.web.Results;
import com.nageoffer.onecoupon.merchant.admin.common.enums.CouponTaskStatusEnum;
import com.nageoffer.onecoupon.merchant.admin.dao.entity.CouponTaskDO;
import com.nageoffer.onecoupon.merchant.admin.dao.mapper.CouponTaskMapper;
import com.nageoffer.onecoupon.merchant.admin.mq.event.CouponTaskExecuteEvent;
import com.nageoffer.onecoupon.merchant.admin.mq.producer.CouponTaskActualExecuteProducer;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.List;

/**
 * 优惠券推送任务扫描定时发送记录 XXL-Job 处理器
 * <p>
 * 作者：马丁
 * 加项目群：早加入就是优势！500人内部项目群，分享的知识总有你需要的 <a href="https://t.zsxq.com/cw7b9" />
 * 开发时间：2024-07-12
 */
@Component
@RequiredArgsConstructor
@RestController // 为了保障快速启动，可通过 Swagger 方式访问接口，可以减少一个中间件 XXL-Job
@Tag(name = "优惠券定时推送任务") // 为了保障快速启动，可通过 Swagger 方式访问接口，可以减少一个中间件 XXL-Job
public class CouponTaskJobHandler extends IJobHandler {

    private final CouponTaskMapper couponTaskMapper;
    private final CouponTaskActualExecuteProducer couponTaskActualExecuteProducer;

    private static final int MAX_LIMIT = 100;

    @SneakyThrows
    @Operation(summary = "执行优惠券定时推送") // 为了保障快速启动，可通过 Swagger 方式访问接口，可以减少一个中间件 XXL-Job
    @GetMapping("/api/merchant-admin/other/coupon-task/job") // 为了保障快速启动，可通过 Swagger 方式访问接口，可以减少一个中间件 XXL-Job
    public Result<Void> webExecute() {
        execute();
        return Results.success();
    }

    @XxlJob(value = "couponTemplateTask")
    public void execute() throws Exception {
        long initId = 0;
        Date now = new Date();

        while (true) {
            // 获取已到执行时间待执行的优惠券定时分发任务
            List<CouponTaskDO> couponTaskDOList = fetchPendingTasks(initId, now);

            if (CollUtil.isEmpty(couponTaskDOList)) {
                break;
            }

            // 调用分发服务对用户发送优惠券
            for (CouponTaskDO each : couponTaskDOList) {
                distributeCoupon(each);
            }

            // 查询出来的数据如果小于 MAX_LIMIT 意味着后面将不再有数据，返回即可
            if (couponTaskDOList.size() < MAX_LIMIT) {
                break;
            }

            // 更新 initId 为当前列表中最大 ID
            initId = couponTaskDOList.stream()
                    .mapToLong(CouponTaskDO::getId)
                    .max()
                    .orElse(initId);
        }
    }

    private void distributeCoupon(CouponTaskDO couponTask) {
        // 修改延时执行推送任务任务状态为执行中
        CouponTaskDO couponTaskDO = CouponTaskDO.builder()
                .id(couponTask.getId())
                .status(CouponTaskStatusEnum.IN_PROGRESS.getStatus())
                .build();
        couponTaskMapper.updateById(couponTaskDO);
        // 通过消息队列发送消息，由分发服务消费者消费该消息
        CouponTaskExecuteEvent couponTaskExecuteEvent = CouponTaskExecuteEvent.builder()
                .couponTaskId(couponTask.getId())
                .build();
        couponTaskActualExecuteProducer.sendMessage(couponTaskExecuteEvent);
    }

    private List<CouponTaskDO> fetchPendingTasks(long initId, Date now) {
        LambdaQueryWrapper<CouponTaskDO> queryWrapper = Wrappers.lambdaQuery(CouponTaskDO.class)
                .eq(CouponTaskDO::getStatus, CouponTaskStatusEnum.PENDING.getStatus())
                .le(CouponTaskDO::getSendTime, now)
                .gt(CouponTaskDO::getId, initId)
                .last("LIMIT " + MAX_LIMIT);
        return couponTaskMapper.selectList(queryWrapper);
    }
}
