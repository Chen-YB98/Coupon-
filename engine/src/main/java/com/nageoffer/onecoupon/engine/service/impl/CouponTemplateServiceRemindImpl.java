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

package com.nageoffer.onecoupon.engine.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.nageoffer.onecoupon.engine.common.context.UserContext;
import com.nageoffer.onecoupon.engine.dao.entity.CouponTemplateDO;
import com.nageoffer.onecoupon.engine.dao.entity.CouponTemplateRemindDO;
import com.nageoffer.onecoupon.engine.dao.mapper.CouponTemplateRemindMapper;
import com.nageoffer.onecoupon.engine.dto.req.CouponTemplateQueryReqDTO;
import com.nageoffer.onecoupon.engine.dto.req.CouponTemplateRemindCancelReqDTO;
import com.nageoffer.onecoupon.engine.dto.req.CouponTemplateRemindCreateReqDTO;
import com.nageoffer.onecoupon.engine.dto.req.CouponTemplateRemindQueryReqDTO;
import com.nageoffer.onecoupon.engine.dto.resp.CouponTemplateQueryRespDTO;
import com.nageoffer.onecoupon.engine.dto.resp.CouponTemplateRemindQueryRespDTO;
import com.nageoffer.onecoupon.engine.mq.event.CouponTemplateRemindDelayEvent;
import com.nageoffer.onecoupon.engine.mq.producer.CouponTemplateRemindDelayProducer;
import com.nageoffer.onecoupon.engine.service.CouponTemplateRemindService;
import com.nageoffer.onecoupon.engine.service.CouponTemplateService;
import com.nageoffer.onecoupon.engine.service.handler.remind.dto.CouponTemplateRemindDTO;
import com.nageoffer.onecoupon.engine.toolkit.CouponTemplateRemindUtil;
import com.nageoffer.onecoupon.framework.exception.ClientException;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RBloomFilter;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.nageoffer.onecoupon.engine.common.constant.EngineRedisConstant.USER_COUPON_TEMPLATE_REMIND_INFORMATION;

/**
 * 优惠券预约提醒业务逻辑实现层
 * <p>
 * 作者：优雅
 * 加项目群：早加入就是优势！500人内部项目群，分享的知识总有你需要的 <a href="https://t.zsxq.com/cw7b9" />
 * 开发时间：2024-07-16
 */
@Service
@RequiredArgsConstructor
public class CouponTemplateServiceRemindImpl extends ServiceImpl<CouponTemplateRemindMapper, CouponTemplateRemindDO> implements CouponTemplateRemindService {

    private final CouponTemplateRemindMapper couponTemplateRemindMapper;
    private final CouponTemplateService couponTemplateService;
    private final RBloomFilter<String> cancelRemindBloomFilter;
    private final CouponTemplateRemindDelayProducer couponTemplateRemindDelayProducer;
    private final StringRedisTemplate stringRedisTemplate;

    @Override
    @Transactional
    public void createCouponRemind(CouponTemplateRemindCreateReqDTO requestParam) {
        // 验证优惠券是否存在，避免缓存穿透问题并获取优惠券开抢时间
        CouponTemplateQueryRespDTO couponTemplate = couponTemplateService
                .findCouponTemplate(new CouponTemplateQueryReqDTO(requestParam.getShopNumber(), requestParam.getCouponTemplateId()));
        if (couponTemplate.getValidStartTime().before(new Date())) {
            throw new ClientException("无法预约已开始领取的优惠券");
        }

        // 查询用户是否已经预约过优惠券的提醒信息
        LambdaQueryWrapper<CouponTemplateRemindDO> queryWrapper = Wrappers.lambdaQuery(CouponTemplateRemindDO.class)
                .eq(CouponTemplateRemindDO::getUserId, UserContext.getUserId())
                .eq(CouponTemplateRemindDO::getCouponTemplateId, requestParam.getCouponTemplateId());
        CouponTemplateRemindDO couponTemplateRemindDO = couponTemplateRemindMapper.selectOne(queryWrapper);

        // 如果没创建过提醒
        if (couponTemplateRemindDO == null) {
            couponTemplateRemindDO = BeanUtil.toBean(requestParam, CouponTemplateRemindDO.class);

            // 设置优惠券开抢时间信息
            couponTemplateRemindDO.setStartTime(couponTemplate.getValidStartTime());
            couponTemplateRemindDO.setInformation(CouponTemplateRemindUtil.calculateBitMap(requestParam.getRemindTime(), requestParam.getType()));
            couponTemplateRemindDO.setUserId(Long.parseLong(UserContext.getUserId()));

            couponTemplateRemindMapper.insert(couponTemplateRemindDO);
        } else {
            Long information = couponTemplateRemindDO.getInformation();
            Long bitMap = CouponTemplateRemindUtil.calculateBitMap(requestParam.getRemindTime(), requestParam.getType());
            if ((information & bitMap) != 0L) {
                throw new ClientException("已经创建过该提醒了");
            }
            couponTemplateRemindDO.setInformation(information ^ bitMap);

            couponTemplateRemindMapper.update(couponTemplateRemindDO, queryWrapper);
        }

        // 发送预约提醒抢购优惠券延时消息
        CouponTemplateRemindDelayEvent couponRemindDelayEvent = CouponTemplateRemindDelayEvent.builder()
                .couponTemplateId(couponTemplate.getId())
                .userId(UserContext.getUserId())
                .contact(UserContext.getUserId())
                .shopNumber(couponTemplate.getShopNumber())
                .type(requestParam.getType())
                .remindTime(requestParam.getRemindTime())
                .startTime(couponTemplate.getValidStartTime())
                .delayTime(DateUtil.offsetMinute(couponTemplate.getValidStartTime(), -requestParam.getRemindTime()).getTime())
                .build();
        couponTemplateRemindDelayProducer.sendMessage(couponRemindDelayEvent);
    }

    @Override
    public List<CouponTemplateRemindQueryRespDTO> listCouponRemind(CouponTemplateRemindQueryReqDTO requestParam) {
        String value = stringRedisTemplate.opsForValue().get(String.format(USER_COUPON_TEMPLATE_REMIND_INFORMATION, requestParam.getUserId()));
        if (value != null) {
            return JSON.parseArray(value, CouponTemplateRemindQueryRespDTO.class);
        }

        // 查出用户预约的信息
        LambdaQueryWrapper<CouponTemplateRemindDO> queryWrapper = Wrappers.lambdaQuery(CouponTemplateRemindDO.class)
                .eq(CouponTemplateRemindDO::getUserId, requestParam.getUserId());
        List<CouponTemplateRemindDO> couponTemplateRemindDOlist = couponTemplateRemindMapper.selectList(queryWrapper);
        if (CollUtil.isEmpty(couponTemplateRemindDOlist))
            return new ArrayList<>();

        // 根据优惠券 ID 查询优惠券信息
        List<Long> couponIds = couponTemplateRemindDOlist.stream()
                .map(CouponTemplateRemindDO::getCouponTemplateId)
                .toList();
        List<Long> shopNumbers = couponTemplateRemindDOlist.stream()
                .map(CouponTemplateRemindDO::getShopNumber)
                .toList();
        List<CouponTemplateDO> couponTemplateDOList = couponTemplateService.listCouponTemplateByIds(couponIds, shopNumbers);
        List<CouponTemplateRemindQueryRespDTO> actualResult = BeanUtil.copyToList(couponTemplateDOList, CouponTemplateRemindQueryRespDTO.class);

        // 填充响应结果的其它信息
        actualResult.forEach(each -> {
            // 找到当前优惠券对应的预约提醒信息
            couponTemplateRemindDOlist.stream()
                    .filter(i -> i.getCouponTemplateId().equals(each.getId()))
                    .findFirst()
                    .ifPresent(i -> {
                        // 解析并填充预约提醒信息
                        CouponTemplateRemindUtil.fillRemindInformation(each, i.getInformation());
                    });
        });

        stringRedisTemplate.opsForValue().set(String.format(USER_COUPON_TEMPLATE_REMIND_INFORMATION, requestParam.getUserId()), JSON.toJSONString(actualResult), 1, TimeUnit.MINUTES);
        return actualResult;
    }

    @Override
    @Transactional
    public void cancelCouponRemind(CouponTemplateRemindCancelReqDTO requestParam) {
        // 验证优惠券是否存在，避免缓存穿透问题并获取优惠券开抢时间
        CouponTemplateQueryRespDTO couponTemplate = couponTemplateService
                .findCouponTemplate(new CouponTemplateQueryReqDTO(requestParam.getShopNumber(), requestParam.getCouponTemplateId()));
        if (couponTemplate.getValidStartTime().before(new Date())) {
            throw new ClientException("无法取消已开始领取的优惠券预约");
        }

        LambdaQueryWrapper<CouponTemplateRemindDO> queryWrapper = Wrappers.lambdaQuery(CouponTemplateRemindDO.class)
                .eq(CouponTemplateRemindDO::getUserId, UserContext.getUserId())
                .eq(CouponTemplateRemindDO::getCouponTemplateId, requestParam.getCouponTemplateId());
        CouponTemplateRemindDO couponTemplateRemindDO = couponTemplateRemindMapper.selectOne(queryWrapper);
        if (couponTemplateRemindDO == null) {
            throw new ClientException("优惠券模板预约信息不存在");
        }

        // 计算 BitMap 信息
        Long bitMap = CouponTemplateRemindUtil.calculateBitMap(requestParam.getRemindTime(), requestParam.getType());
        if ((bitMap & couponTemplateRemindDO.getInformation()) == 0L) {
            throw new ClientException("您没有预约该时间点的提醒");
        }

        bitMap ^= couponTemplateRemindDO.getInformation();
        queryWrapper.eq(CouponTemplateRemindDO::getInformation, couponTemplateRemindDO.getInformation());
        if (bitMap.equals(0L)) {
            // 如果新 BitMap 信息是 0，说明已经没有预约提醒了，可以直接删除
            if (couponTemplateRemindMapper.delete(queryWrapper) == 0) {
                // MySQL 乐观锁进行删除，如果删除失败，说明用户可能同时正在进行删除、新增提醒操作
                throw new ClientException("取消提醒失败，请刷新页面后重试");
            }
        } else {
            // 虽然删除了这个预约提醒，但还有其它提醒，那就更新数据库
            couponTemplateRemindDO.setInformation(bitMap);
            if (couponTemplateRemindMapper.update(couponTemplateRemindDO, queryWrapper) == 0) {
                // MySQL 乐观锁进行更新，如果更新失败，说明用户可能同时正在进行删除、新增提醒操作
                throw new ClientException("取消提醒失败，请刷新页面后重试");
            }
        }

        // 取消提醒这个信息添加到布隆过滤器中
        cancelRemindBloomFilter.add(String.valueOf(Objects.hash(requestParam.getCouponTemplateId(), UserContext.getUserId(), requestParam.getRemindTime(), requestParam.getType())));

        // 删除用户预约提醒的缓存信息，通过更新数据库删除缓存策略保障数据库和缓存一致性
        stringRedisTemplate.delete(String.format(USER_COUPON_TEMPLATE_REMIND_INFORMATION, UserContext.getUserId()));
    }

    @Override
    public boolean isCancelRemind(CouponTemplateRemindDTO requestParam) {
        if (!cancelRemindBloomFilter.contains(String.valueOf(Objects.hash(requestParam.getCouponTemplateId(), requestParam.getUserId(), requestParam.getRemindTime(), requestParam.getType())))) {
            // 布隆过滤器中不存在，说明没取消提醒，此时已经能挡下大部分请求
            return false;
        }

        // 对于少部分的“取消了预约”，可能是误判，此时需要去数据库中查找
        LambdaQueryWrapper<CouponTemplateRemindDO> queryWrapper = Wrappers.lambdaQuery(CouponTemplateRemindDO.class)
                .eq(CouponTemplateRemindDO::getUserId, requestParam.getUserId())
                .eq(CouponTemplateRemindDO::getCouponTemplateId, requestParam.getCouponTemplateId());
        CouponTemplateRemindDO couponTemplateRemindDO = couponTemplateRemindMapper.selectOne(queryWrapper);
        if (couponTemplateRemindDO == null) {
            // 数据库中没该条预约提醒，说明被取消
            return true;
        }

        // 即使存在数据，也要检查该类型的该时间点是否有提醒
        Long information = couponTemplateRemindDO.getInformation();
        Long bitMap = CouponTemplateRemindUtil.calculateBitMap(requestParam.getRemindTime(), requestParam.getType());

        // 按位与等于 0 说明用户取消了预约
        return (bitMap & information) == 0L;
    }
}
