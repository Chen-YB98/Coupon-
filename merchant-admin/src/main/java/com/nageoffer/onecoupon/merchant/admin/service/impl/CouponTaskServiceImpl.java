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

package com.nageoffer.onecoupon.merchant.admin.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.excel.EasyExcel;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.nageoffer.onecoupon.framework.exception.ClientException;
import com.nageoffer.onecoupon.merchant.admin.common.context.UserContext;
import com.nageoffer.onecoupon.merchant.admin.common.enums.CouponTaskSendTypeEnum;
import com.nageoffer.onecoupon.merchant.admin.common.enums.CouponTaskStatusEnum;
import com.nageoffer.onecoupon.merchant.admin.dao.entity.CouponTaskDO;
import com.nageoffer.onecoupon.merchant.admin.dao.mapper.CouponTaskMapper;
import com.nageoffer.onecoupon.merchant.admin.dto.req.CouponTaskCreateReqDTO;
import com.nageoffer.onecoupon.merchant.admin.dto.req.CouponTaskPageQueryReqDTO;
import com.nageoffer.onecoupon.merchant.admin.dto.resp.CouponTaskPageQueryRespDTO;
import com.nageoffer.onecoupon.merchant.admin.dto.resp.CouponTaskQueryRespDTO;
import com.nageoffer.onecoupon.merchant.admin.dto.resp.CouponTemplateQueryRespDTO;
import com.nageoffer.onecoupon.merchant.admin.mq.event.CouponTaskExecuteEvent;
import com.nageoffer.onecoupon.merchant.admin.mq.producer.CouponTaskActualExecuteProducer;
import com.nageoffer.onecoupon.merchant.admin.service.CouponTaskService;
import com.nageoffer.onecoupon.merchant.admin.service.CouponTemplateService;
import com.nageoffer.onecoupon.merchant.admin.service.handler.excel.RowCountListener;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RBlockingDeque;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 优惠券推送业务逻辑实现层

 */
@Service
@RequiredArgsConstructor
public class CouponTaskServiceImpl extends ServiceImpl<CouponTaskMapper, CouponTaskDO> implements CouponTaskService {

    private final CouponTemplateService couponTemplateService;
    private final CouponTaskMapper couponTaskMapper;
    private final RedissonClient redissonClient;
    private final CouponTaskActualExecuteProducer couponTaskActualExecuteProducer;

    /**
     * 为什么这里拒绝策略使用直接丢弃任务？因为在发送任务时如果遇到发送数量为空，会重新进行统计
     */
    private final ExecutorService executorService = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors() << 1,
            60,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadPoolExecutor.DiscardPolicy()
    );

    @Transactional(rollbackFor = Exception.class)
    @Override
    public void createCouponTask(CouponTaskCreateReqDTO requestParam) {
        // 验证非空参数
        // 验证参数是否正确，比如文件地址是否为我们期望的格式等
        // 验证参数依赖关系，比如选择定时发送，发送时间是否不为空等
        CouponTemplateQueryRespDTO couponTemplate = couponTemplateService.findCouponTemplateById(requestParam.getCouponTemplateId());
        if (couponTemplate == null) {
            throw new ClientException("优惠券模板不存在，请检查提交信息是否正确");
        }
        // ......

        // 构建优惠券推送任务数据库持久层实体
        CouponTaskDO couponTaskDO = BeanUtil.copyProperties(requestParam, CouponTaskDO.class);
        // 批次id
        couponTaskDO.setBatchId(IdUtil.getSnowflakeNextId());
        couponTaskDO.setOperatorId(Long.parseLong(UserContext.getUserId()));
        couponTaskDO.setShopNumber(UserContext.getShopNumber());
        couponTaskDO.setStatus(
                Objects.equals(requestParam.getSendType(), CouponTaskSendTypeEnum.IMMEDIATE.getType())
                        ? CouponTaskStatusEnum.IN_PROGRESS.getStatus()
                        : CouponTaskStatusEnum.PENDING.getStatus()
        );

        // 保存优惠券推送任务记录到数据库
        couponTaskMapper.insert(couponTaskDO);

        // 为什么需要统计行数？因为发送后需要比对所有优惠券是否都已发放到用户账号
        // 100 万数据大概需要 4 秒才能返回前端，如果加上验证将会时间更长，所以这里将最耗时的统计操作异步化
        JSONObject delayJsonObject = JSONObject
                .of("fileAddress", requestParam.getFileAddress(), "couponTaskId", couponTaskDO.getId());
        executorService.execute(() -> refreshCouponTaskSendNum(delayJsonObject));

        // 假设刚把消息提交到线程池，突然应用宕机了，我们通过延迟队列进行兜底 Refresh
        RBlockingDeque<Object> blockingDeque = redissonClient.getBlockingDeque("COUPON_TASK_SEND_NUM_DELAY_QUEUE");
        RDelayedQueue<Object> delayedQueue = redissonClient.getDelayedQueue(blockingDeque);
        // 这里延迟时间设置 20 秒，原因是我们笃定上面线程池 20 秒之内就能结束任务
        delayedQueue.offer(delayJsonObject, 20, TimeUnit.SECONDS);

        // 如果是立即发送任务，直接调用消息队列进行发送流程
        if (Objects.equals(requestParam.getSendType(), CouponTaskSendTypeEnum.IMMEDIATE.getType())) {
            // 执行优惠券推送业务，正式向用户发放优惠券
            CouponTaskExecuteEvent couponTaskExecuteEvent = CouponTaskExecuteEvent.builder()
                    .couponTaskId(couponTaskDO.getId())
                    .build();
            couponTaskActualExecuteProducer.sendMessage(couponTaskExecuteEvent);
        }
    }

    @Override
    public IPage<CouponTaskPageQueryRespDTO> pageQueryCouponTask(CouponTaskPageQueryReqDTO requestParam) {
        // 构建分页查询模板 LambdaQueryWrapper
        LambdaQueryWrapper<CouponTaskDO> queryWrapper = Wrappers.lambdaQuery(CouponTaskDO.class)
                .eq(CouponTaskDO::getShopNumber, UserContext.getShopNumber())
                .eq(StrUtil.isNotBlank(requestParam.getBatchId()), CouponTaskDO::getBatchId, requestParam.getBatchId())
                .like(StrUtil.isNotBlank(requestParam.getTaskName()), CouponTaskDO::getTaskName, requestParam.getTaskName())
                .eq(StrUtil.isNotBlank(requestParam.getCouponTemplateId()), CouponTaskDO::getCouponTemplateId, requestParam.getCouponTemplateId())
                .eq(Objects.nonNull(requestParam.getStatus()), CouponTaskDO::getStatus, requestParam.getStatus());

        // MyBatis-Plus 分页查询优惠券推送任务信息
        IPage<CouponTaskDO> selectPage = couponTaskMapper.selectPage(requestParam, queryWrapper);

        // 转换数据库持久层对象为优惠券模板返回参数
        return selectPage.convert(each -> BeanUtil.toBean(each, CouponTaskPageQueryRespDTO.class));
    }

    @Override
    public CouponTaskQueryRespDTO findCouponTaskById(String taskId) {
        CouponTaskDO couponTaskDO = couponTaskMapper.selectById(taskId);
        return BeanUtil.toBean(couponTaskDO, CouponTaskQueryRespDTO.class);
    }

    private void refreshCouponTaskSendNum(JSONObject delayJsonObject) {
        // 通过 EasyExcel 监听器获取 Excel 中所有行数
        RowCountListener listener = new RowCountListener();
        EasyExcel.read(delayJsonObject.getString("fileAddress"), listener).sheet().doRead();
        int totalRows = listener.getRowCount();

        // 刷新优惠券推送记录中发送行数
        CouponTaskDO updateCouponTaskDO = CouponTaskDO.builder()
                .id(delayJsonObject.getLong("couponTaskId"))
                .sendNum(totalRows)
                .build();
        couponTaskMapper.updateById(updateCouponTaskDO);
    }

    /**
     * 因为静态内部类的 Bean 注入有问题，所以我们这里直接 new 对象运行即可
     * 如果按照上一版本的方式写，refreshCouponTaskSendNum 方法中 couponTaskMapper 为空
     */
    @PostConstruct
    public void init() {
        new RefreshCouponTaskDelayQueueRunner(this, couponTaskMapper, redissonClient).run();
    }

    /**
     * 优惠券延迟刷新发送条数兜底消费者｜这是兜底策略，一般来说不会执行这段逻辑
     * 如果延迟消息没有持久化成功，或者 Redis 挂了怎么办？后续可以人工处理
     * <p>
     * 作者：马丁
     * 加项目群：早加入就是优势！500人内部项目群，分享的知识总有你需要的 <a href="https://t.zsxq.com/cw7b9" />
     * 开发时间：2024-07-12
     */
    @RequiredArgsConstructor
    static class RefreshCouponTaskDelayQueueRunner {

        private final CouponTaskServiceImpl couponTaskService;
        private final CouponTaskMapper couponTaskMapper;
        private final RedissonClient redissonClient;

        public void run() {
            Executors.newSingleThreadExecutor(
                            runnable -> {
                                Thread thread = new Thread(runnable);
                                thread.setName("delay_coupon-task_send-num_consumer");
                                thread.setDaemon(Boolean.TRUE);
                                return thread;
                            })
                    .execute(() -> {
                        RBlockingDeque<JSONObject> blockingDeque = redissonClient.getBlockingDeque("COUPON_TASK_SEND_NUM_DELAY_QUEUE");
                        for (; ; ) {
                            try {
                                // 获取延迟队列已到达时间元素
                                JSONObject delayJsonObject = blockingDeque.take();
                                if (delayJsonObject != null) {
                                    // 获取优惠券推送记录，查看发送条数是否已经有值，有的话代表上面线程池已经处理完成，无需再处理
                                    CouponTaskDO couponTaskDO = couponTaskMapper.selectById(delayJsonObject.getLong("couponTaskId"));
                                    if (couponTaskDO.getSendNum() == null) {
                                        couponTaskService.refreshCouponTaskSendNum(delayJsonObject);
                                    }
                                }
                            } catch (Throwable ignored) {
                            }
                        }
                    });
        }
    }
}
