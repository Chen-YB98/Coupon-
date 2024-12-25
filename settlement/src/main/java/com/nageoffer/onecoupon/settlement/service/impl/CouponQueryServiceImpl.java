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

package com.nageoffer.onecoupon.settlement.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.nageoffer.onecoupon.framework.config.RedisDistributedProperties;
import com.nageoffer.onecoupon.framework.exception.ClientException;
import com.nageoffer.onecoupon.settlement.common.context.UserContext;
import com.nageoffer.onecoupon.settlement.dto.req.QueryCouponGoodsReqDTO;
import com.nageoffer.onecoupon.settlement.dto.req.QueryCouponsReqDTO;
import com.nageoffer.onecoupon.settlement.dto.resp.CouponTemplateQueryRespDTO;
import com.nageoffer.onecoupon.settlement.dto.resp.QueryCouponsDetailRespDTO;
import com.nageoffer.onecoupon.settlement.dto.resp.QueryCouponsRespDTO;
import com.nageoffer.onecoupon.settlement.service.CouponQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.nageoffer.onecoupon.settlement.common.constant.EngineRedisConstant.COUPON_TEMPLATE_KEY;
import static com.nageoffer.onecoupon.settlement.common.constant.EngineRedisConstant.USER_COUPON_TEMPLATE_LIST_KEY;

/**
 * 查询用户可用优惠券列表接口
 * <p>
 * 作者：Henry Wan
 * 加项目群：早加入就是优势！500人内部项目群，分享的知识总有你需要的 <a href="https://t.zsxq.com/cw7b9" />
 * 开发时间：2024-07-25
 */
@Service
@RequiredArgsConstructor
public class CouponQueryServiceImpl implements CouponQueryService {

    private final RedisDistributedProperties redisDistributedProperties;
    private final StringRedisTemplate stringRedisTemplate;

    // 在我们本次的业务场景中，属于是 CPU 密集型任务，设置 CPU 的核心数即可
    private final ExecutorService executorService = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors(),
            9999,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadPoolExecutor.CallerRunsPolicy()
    );

    @Override
    public QueryCouponsRespDTO listQueryUserCoupons(QueryCouponsReqDTO requestParam) {
        // Step 1: 获取 Redis 中的用户优惠券列表
        Set<String> rangeUserCoupons = stringRedisTemplate.opsForZSet().range(
                String.format(USER_COUPON_TEMPLATE_LIST_KEY, UserContext.getUserId()), 0, -1);

        if (rangeUserCoupons == null || rangeUserCoupons.isEmpty()) {
            return QueryCouponsRespDTO.builder()
                    .availableCouponList(new ArrayList<>())
                    .notAvailableCouponList(new ArrayList<>())
                    .build();
        }

        // 构建 Redis Key 列表
        List<String> couponTemplateIds = rangeUserCoupons.stream()
                .map(each -> StrUtil.split(each, "_").get(0))
                .map(each -> redisDistributedProperties.getPrefix() + String.format(COUPON_TEMPLATE_KEY, each))
                .toList();

        // 同步获取 Redis 数据并进行解析、转换和分区
        List<Object> rawCouponDataList = stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            couponTemplateIds.forEach(each -> connection.hashCommands().hGetAll(each.getBytes()));
            return null;
        });

        // 解析 Redis 数据，并按 `goods` 字段进行分区处理
        Map<Boolean, List<CouponTemplateQueryRespDTO>> partitioned = JSON.parseArray(JSON.toJSONString(rawCouponDataList), CouponTemplateQueryRespDTO.class)
                .stream()
                .collect(Collectors.partitioningBy(coupon -> StrUtil.isEmpty(coupon.getGoods())));

        // 拆分后的两个列表
        List<CouponTemplateQueryRespDTO> goodsEmptyList = partitioned.get(true); // goods 为空的列表
        List<CouponTemplateQueryRespDTO> goodsNotEmptyList = partitioned.get(false); // goods 不为空的列表

        // 针对当前订单可用/不可用的优惠券列表
        List<QueryCouponsDetailRespDTO> availableCouponList = Collections.synchronizedList(new ArrayList<>());
        List<QueryCouponsDetailRespDTO> notAvailableCouponList = Collections.synchronizedList(new ArrayList<>());

        // Step 2: 并行处理 goodsEmptyList 和 goodsNotEmptyList 中的每个元素
        CompletableFuture<Void> emptyGoodsTasks = CompletableFuture.allOf(
                goodsEmptyList.stream()
                        .map(each -> CompletableFuture.runAsync(() -> {
                            QueryCouponsDetailRespDTO resultCouponDetail = BeanUtil.toBean(each, QueryCouponsDetailRespDTO.class);
                            JSONObject jsonObject = JSON.parseObject(each.getConsumeRule());
                            handleCouponLogic(resultCouponDetail, jsonObject, requestParam.getOrderAmount(), availableCouponList, notAvailableCouponList);
                        }, executorService))
                        .toArray(CompletableFuture[]::new)
        );

        Map<String, QueryCouponGoodsReqDTO> goodsRequestMap = requestParam.getGoodsList().stream()
                .collect(Collectors.toMap(QueryCouponGoodsReqDTO::getGoodsNumber, Function.identity()));
        CompletableFuture<Void> notEmptyGoodsTasks = CompletableFuture.allOf(
                goodsNotEmptyList.stream()
                        .map(each -> CompletableFuture.runAsync(() -> {
                            QueryCouponsDetailRespDTO resultCouponDetail = BeanUtil.toBean(each, QueryCouponsDetailRespDTO.class);
                            QueryCouponGoodsReqDTO couponGoods = goodsRequestMap.get(each.getGoods());
                            if (couponGoods == null) {
                                notAvailableCouponList.add(resultCouponDetail);
                            } else {
                                JSONObject jsonObject = JSON.parseObject(each.getConsumeRule());
                                handleCouponLogic(resultCouponDetail, jsonObject, couponGoods.getGoodsAmount(), availableCouponList, notAvailableCouponList);
                            }
                        }, executorService))
                        .toArray(CompletableFuture[]::new)
        );

        // Step 3: 等待两个异步任务集合完成
        CompletableFuture.allOf(emptyGoodsTasks, notEmptyGoodsTasks)
                .thenRun(() -> {
                    // 与业内标准一致，按最终优惠力度从大到小排序
                    availableCouponList.sort((c1, c2) -> c2.getCouponAmount().compareTo(c1.getCouponAmount()));
                })
                .join();

        // 构建最终结果并返回
        return QueryCouponsRespDTO.builder()
                .availableCouponList(availableCouponList)
                .notAvailableCouponList(notAvailableCouponList)
                .build();
    }

    // 优惠券判断逻辑，根据条件判断放入可用或不可用列表
    private void handleCouponLogic(QueryCouponsDetailRespDTO resultCouponDetail, JSONObject jsonObject, BigDecimal amount,
                                   List<QueryCouponsDetailRespDTO> availableCouponList, List<QueryCouponsDetailRespDTO> notAvailableCouponList) {
        BigDecimal termsOfUse = jsonObject.getBigDecimal("termsOfUse");
        BigDecimal maximumDiscountAmount = jsonObject.getBigDecimal("maximumDiscountAmount");

        switch (resultCouponDetail.getType()) {
            case 0: // 立减券
                resultCouponDetail.setCouponAmount(maximumDiscountAmount);
                availableCouponList.add(resultCouponDetail);
                break;
            case 1: // 满减券
                if (amount.compareTo(termsOfUse) >= 0) {
                    resultCouponDetail.setCouponAmount(maximumDiscountAmount);
                    availableCouponList.add(resultCouponDetail);
                } else {
                    notAvailableCouponList.add(resultCouponDetail);
                }
                break;
            case 2: // 折扣券
                if (amount.compareTo(termsOfUse) >= 0) {
                    BigDecimal discountRate = jsonObject.getBigDecimal("discountRate");
                    BigDecimal multiply = amount.multiply(discountRate);
                    if (multiply.compareTo(maximumDiscountAmount) >= 0) {
                        resultCouponDetail.setCouponAmount(maximumDiscountAmount);
                    } else {
                        resultCouponDetail.setCouponAmount(multiply);
                    }
                    availableCouponList.add(resultCouponDetail);
                } else {
                    notAvailableCouponList.add(resultCouponDetail);
                }
                break;
            default:
                throw new ClientException("无效的优惠券类型");
        }
    }

    /**
     * 单线程版本，好理解一些。上面的多线程就是基于这个版本演进的
     */
    public QueryCouponsRespDTO listQueryUserCouponsBySync(QueryCouponsReqDTO requestParam) {
        Set<String> rangeUserCoupons = stringRedisTemplate.opsForZSet().range(String.format(USER_COUPON_TEMPLATE_LIST_KEY, UserContext.getUserId()), 0, -1);

        List<String> couponTemplateIds = rangeUserCoupons.stream()
                .map(each -> StrUtil.split(each, "_").get(0))
                .map(each -> redisDistributedProperties.getPrefix() + String.format(COUPON_TEMPLATE_KEY, each))
                .toList();
        List<Object> couponTemplateList = stringRedisTemplate.executePipelined((RedisCallback<String>) connection -> {
            couponTemplateIds.forEach(each -> connection.hashCommands().hGetAll(each.getBytes()));
            return null;
        });

        List<CouponTemplateQueryRespDTO> couponTemplateDTOList = JSON.parseArray(JSON.toJSONString(couponTemplateList), CouponTemplateQueryRespDTO.class);
        Map<Boolean, List<CouponTemplateQueryRespDTO>> partitioned = couponTemplateDTOList.stream()
                .collect(Collectors.partitioningBy(coupon -> StrUtil.isEmpty(coupon.getGoods())));

        // 拆分后的两个列表
        List<CouponTemplateQueryRespDTO> goodsEmptyList = partitioned.get(true); // goods 为空的列表
        List<CouponTemplateQueryRespDTO> goodsNotEmptyList = partitioned.get(false); // goods 不为空的列表

        // 针对当前订单可用/不可用的优惠券列表
        List<QueryCouponsDetailRespDTO> availableCouponList = new ArrayList<>();
        List<QueryCouponsDetailRespDTO> notAvailableCouponList = new ArrayList<>();

        goodsEmptyList.forEach(each -> {
            JSONObject jsonObject = JSON.parseObject(each.getConsumeRule());
            QueryCouponsDetailRespDTO resultQueryCouponDetail = BeanUtil.toBean(each, QueryCouponsDetailRespDTO.class);
            BigDecimal maximumDiscountAmount = jsonObject.getBigDecimal("maximumDiscountAmount");
            switch (each.getType()) {
                case 0: // 立减券
                    resultQueryCouponDetail.setCouponAmount(maximumDiscountAmount);
                    availableCouponList.add(resultQueryCouponDetail);
                    break;
                case 1: // 满减券
                    // orderAmount 大于或等于 termsOfUse
                    if (requestParam.getOrderAmount().compareTo(jsonObject.getBigDecimal("termsOfUse")) >= 0) {
                        resultQueryCouponDetail.setCouponAmount(maximumDiscountAmount);
                        availableCouponList.add(resultQueryCouponDetail);
                    } else {
                        notAvailableCouponList.add(resultQueryCouponDetail);
                    }
                    break;
                case 2: // 折扣券
                    // orderAmount 大于或等于 termsOfUse
                    if (requestParam.getOrderAmount().compareTo(jsonObject.getBigDecimal("termsOfUse")) >= 0) {
                        BigDecimal multiply = requestParam.getOrderAmount().multiply(jsonObject.getBigDecimal("discountRate"));
                        if (multiply.compareTo(maximumDiscountAmount) >= 0) {
                            resultQueryCouponDetail.setCouponAmount(maximumDiscountAmount);
                        } else {
                            resultQueryCouponDetail.setCouponAmount(multiply);
                        }
                        availableCouponList.add(resultQueryCouponDetail);
                    } else {
                        notAvailableCouponList.add(resultQueryCouponDetail);
                    }
                    break;
                default:
                    throw new ClientException("无效的优惠券类型");
            }
        });

        Map<String, QueryCouponGoodsReqDTO> goodsRequestMap = requestParam.getGoodsList().stream()
                .collect(Collectors.toMap(QueryCouponGoodsReqDTO::getGoodsNumber, Function.identity(), (existing, replacement) -> existing));

        goodsNotEmptyList.forEach(each -> {
            QueryCouponGoodsReqDTO couponGoods = goodsRequestMap.get(each.getGoods());
            if (couponGoods == null) {
                notAvailableCouponList.add(BeanUtil.toBean(each, QueryCouponsDetailRespDTO.class));
            }
            JSONObject jsonObject = JSON.parseObject(each.getConsumeRule());
            QueryCouponsDetailRespDTO resultQueryCouponDetail = BeanUtil.toBean(each, QueryCouponsDetailRespDTO.class);
            switch (each.getType()) {
                case 0: // 立减券
                    resultQueryCouponDetail.setCouponAmount(jsonObject.getBigDecimal("maximumDiscountAmount"));
                    availableCouponList.add(resultQueryCouponDetail);
                    break;
                case 1: // 满减券
                    // goodsAmount 大于或等于 termsOfUse
                    if (couponGoods.getGoodsAmount().compareTo(jsonObject.getBigDecimal("termsOfUse")) >= 0) {
                        resultQueryCouponDetail.setCouponAmount(jsonObject.getBigDecimal("maximumDiscountAmount"));
                        availableCouponList.add(resultQueryCouponDetail);
                    } else {
                        notAvailableCouponList.add(resultQueryCouponDetail);
                    }
                    break;
                case 2: // 折扣券
                    // goodsAmount 大于或等于 termsOfUse
                    if (couponGoods.getGoodsAmount().compareTo(jsonObject.getBigDecimal("termsOfUse")) >= 0) {
                        BigDecimal discountRate = jsonObject.getBigDecimal("discountRate");
                        resultQueryCouponDetail.setCouponAmount(couponGoods.getGoodsAmount().multiply(discountRate));
                        availableCouponList.add(resultQueryCouponDetail);
                    } else {
                        notAvailableCouponList.add(resultQueryCouponDetail);
                    }
                    break;
                default:
                    throw new ClientException("无效的优惠券类型");
            }
        });

        // 与业内标准一致，按最终优惠力度从大到小排序
        availableCouponList.sort((c1, c2) -> c2.getCouponAmount().compareTo(c1.getCouponAmount()));

        return QueryCouponsRespDTO.builder()
                .availableCouponList(availableCouponList)
                .notAvailableCouponList(notAvailableCouponList)
                .build();
    }
}