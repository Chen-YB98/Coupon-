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

package com.nageoffer.onecoupon.distribution.service.handler.excel;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.lang.Singleton;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.nageoffer.onecoupon.distribution.common.constant.DistributionRedisConstant;
import com.nageoffer.onecoupon.distribution.common.constant.EngineRedisConstant;
import com.nageoffer.onecoupon.distribution.dao.entity.CouponTaskDO;
import com.nageoffer.onecoupon.distribution.dao.entity.CouponTaskFailDO;
import com.nageoffer.onecoupon.distribution.dao.entity.CouponTemplateDO;
import com.nageoffer.onecoupon.distribution.dao.mapper.CouponTaskFailMapper;
import com.nageoffer.onecoupon.distribution.mq.event.CouponTemplateDistributionEvent;
import com.nageoffer.onecoupon.distribution.mq.producer.CouponExecuteDistributionProducer;
import com.nageoffer.onecoupon.distribution.toolkit.StockDecrementReturnCombinedUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;

import java.util.Map;

/**
 * 优惠券任务读取 Excel 分发监听器
 * <p>
 * 作者：马丁
 * 加项目群：早加入就是优势！500人内部沟通群，分享的知识总有你需要的 <a href="https://t.zsxq.com/cw7b9" />
 * 开发时间：2024-07-14
 */
@RequiredArgsConstructor
public class ReadExcelDistributionListener extends AnalysisEventListener<CouponTaskExcelObject> {

    private final CouponTaskDO couponTaskDO;
    private final CouponTemplateDO couponTemplateDO;
    private final CouponTaskFailMapper couponTaskFailMapper;

    private final StringRedisTemplate stringRedisTemplate;
    private final CouponExecuteDistributionProducer couponExecuteDistributionProducer;

    private int rowCount = 1;
    private final static String STOCK_DECREMENT_AND_BATCH_SAVE_USER_RECORD_LUA_PATH = "lua/stock_decrement_and_batch_save_user_record.lua";
    private final static int BATCH_USER_COUPON_SIZE = 5000;

    @Override
    public void invoke(CouponTaskExcelObject data, AnalysisContext context) {
        Long couponTaskId = couponTaskDO.getId();

        // 获取当前进度，判断是否已经执行过。如果已执行，则跳过即可，防止执行到一半应用宕机
        String templateTaskExecuteProgressKey = String.format(DistributionRedisConstant.TEMPLATE_TASK_EXECUTE_PROGRESS_KEY, couponTaskId);
        String progress = stringRedisTemplate.opsForValue().get(templateTaskExecuteProgressKey);
        if (StrUtil.isNotBlank(progress) && Integer.parseInt(progress) >= rowCount) {
            ++rowCount;
            return;
        }

        // 获取 LUA 脚本，并保存到 Hutool 的单例管理容器，下次直接获取不需要加载
        DefaultRedisScript<Long> buildLuaScript = Singleton.get(STOCK_DECREMENT_AND_BATCH_SAVE_USER_RECORD_LUA_PATH, () -> {
            DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
            redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource(STOCK_DECREMENT_AND_BATCH_SAVE_USER_RECORD_LUA_PATH)));
            redisScript.setResultType(Long.class);
            return redisScript;
        });

        // 执行 LUA 脚本进行扣减库存以及增加 Redis 用户领券记录
        String couponTemplateKey = String.format(EngineRedisConstant.COUPON_TEMPLATE_KEY, couponTemplateDO.getId());
        String batchUserSetKey = String.format(DistributionRedisConstant.TEMPLATE_TASK_EXECUTE_BATCH_USER_KEY, couponTaskId);
        Map<Object, Object> userRowNumMap = MapUtil.builder()
                .put("userId", data.getUserId())
                .put("rowNum", rowCount + 1)
                .build();
        Long combinedFiled = stringRedisTemplate.execute(buildLuaScript, ListUtil.of(couponTemplateKey, batchUserSetKey), JSON.toJSONString(userRowNumMap));

        // firstField 为 false 说明优惠券已经没有库存了
        boolean firstField = StockDecrementReturnCombinedUtil.extractFirstField(combinedFiled);
        if (!firstField) {
            // 同步当前执行进度到缓存
            stringRedisTemplate.opsForValue().set(templateTaskExecuteProgressKey, String.valueOf(rowCount));
            ++rowCount;

            // 添加到 t_coupon_task_fail 并标记错误原因，方便后续查看未成功发送的原因和记录
            Map<Object, Object> objectMap = MapUtil.builder()
                    .put("rowNum", rowCount + 1)
                    .put("cause", "优惠券模板无库存")
                    .build();
            CouponTaskFailDO couponTaskFailDO = CouponTaskFailDO.builder()
                    .batchId(couponTaskDO.getBatchId())
                    .jsonObject(JSON.toJSONString(objectMap, SerializerFeature.WriteMapNullValue))
                    .build();
            couponTaskFailMapper.insert(couponTaskFailDO);
            return;
        }

        // 获取用户领券集合长度
        int batchUserSetSize = StockDecrementReturnCombinedUtil.extractSecondField(combinedFiled.intValue());

        // 如果没有消息通知需求，仅在 batchUserSetSize = BATCH_USER_COUPON_SIZE 时发送消息消费。不满足条件仅记录执行进度即可
        if (batchUserSetSize < BATCH_USER_COUPON_SIZE && StrUtil.isBlank(couponTaskDO.getNotifyType())) {
            // 同步当前 Excel 执行进度到缓存
            stringRedisTemplate.opsForValue().set(templateTaskExecuteProgressKey, String.valueOf(rowCount));
            ++rowCount;
            return;
        }

        CouponTemplateDistributionEvent couponTemplateDistributionEvent = CouponTemplateDistributionEvent.builder()
                .userId(data.getUserId())
                .mail(data.getMail())
                .phone(data.getPhone())
                .couponTaskId(couponTaskId)
                .notifyType(couponTaskDO.getNotifyType())
                .shopNumber(couponTaskDO.getShopNumber())
                .couponTemplateId(couponTemplateDO.getId())
                .couponTaskBatchId(couponTaskDO.getBatchId())
                .couponTemplateConsumeRule(couponTemplateDO.getConsumeRule())
                .batchUserSetSize(batchUserSetSize)
                .distributionEndFlag(Boolean.FALSE)
                .build();
        couponExecuteDistributionProducer.sendMessage(couponTemplateDistributionEvent);

        // 同步当前执行进度到缓存
        stringRedisTemplate.opsForValue().set(templateTaskExecuteProgressKey, String.valueOf(rowCount));
        ++rowCount;
    }

    @Override
    public void doAfterAllAnalysed(AnalysisContext context) {
        // 发送 Excel 解析完成标识，即使不满足批量保存的数量也得保存到数据库
        CouponTemplateDistributionEvent couponTemplateExecuteEvent = CouponTemplateDistributionEvent.builder()
                .distributionEndFlag(Boolean.TRUE) // 设置解析完成标识
                .shopNumber(couponTaskDO.getShopNumber())
                .couponTemplateId(couponTemplateDO.getId())
                .couponTemplateConsumeRule(couponTemplateDO.getConsumeRule())
                .couponTaskBatchId(couponTaskDO.getBatchId())
                .couponTaskId(couponTaskDO.getId())
                .build();
        couponExecuteDistributionProducer.sendMessage(couponTemplateExecuteEvent);
    }
}
