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

package com.nageoffer.onecoupon.merchant.admin.service.basics.chain;

import com.nageoffer.onecoupon.merchant.admin.service.handler.filter.CouponTemplateCreateParamBaseVerifyChainFilter;
import com.nageoffer.onecoupon.merchant.admin.service.handler.filter.CouponTemplateCreateParamNotNullChainFilter;
import com.nageoffer.onecoupon.merchant.admin.service.handler.filter.CouponTemplateCreateParamVerifyChainFilter;
import org.springframework.beans.BeansException;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 商家后管责任链上下文容器
 * ApplicationContextAware 接口获取应用上下文，并复制局部变量方便后续使用；CommandLineRunner 项目启动后执行责任链容器的填充工作
 * <p>
 * 作者：马丁
 * 加项目群：早加入就是优势！500人内部项目群，分享的知识总有你需要的 <a href="https://t.zsxq.com/cw7b9" />
 * 开发时间：2024-07-09
 */
@Component
public final class MerchantAdminChainContext<T> implements ApplicationContextAware, CommandLineRunner {

    /**
     * 应用上下文，我们这里通过 Spring IOC 获取 Bean 实例
     */
    private ApplicationContext applicationContext;
    /**
     * 保存商家后管责任链实现类
     * <p>
     * Key：{@link MerchantAdminAbstractChainHandler#mark()}
     * Val：{@link MerchantAdminAbstractChainHandler} 一组责任链实现 Spring Bean 集合
     * <p>
     * 比如有一个优惠券模板创建责任链，实例如下：
     * Key：MERCHANT_ADMIN_CREATE_COUPON_TEMPLATE_KEY
     * Val：
     * - 验证优惠券信息基本参数是否必填 —— 执行器 {@link CouponTemplateCreateParamNotNullChainFilter}
     * - 验证优惠券信息基本参数是否按照格式传递 —— 执行器 {@link CouponTemplateCreateParamBaseVerifyChainFilter}
     * - 验证优惠券信息基本参数是否正确，比如商品数据是否存在等 —— 执行器 {@link CouponTemplateCreateParamVerifyChainFilter}
     * - ......
     */
    private final Map<String, List<MerchantAdminAbstractChainHandler>> abstractChainHandlerContainer = new HashMap<>();

    /**
     * 责任链组件执行
     *
     * @param mark         责任链组件标识
     * @param requestParam 请求参数
     */
    public void handler(String mark, T requestParam) {
        // 根据 mark 标识从责任链容器中获取一组责任链实现 Bean 集合
        List<MerchantAdminAbstractChainHandler> abstractChainHandlers = abstractChainHandlerContainer.get(mark);
        if (CollectionUtils.isEmpty(abstractChainHandlers)) {
            throw new RuntimeException(String.format("[%s] Chain of Responsibility ID is undefined.", mark));
        }
        abstractChainHandlers.forEach(each -> each.handler(requestParam));
    }

    @Override
    public void run(String... args) throws Exception {
        // 从 Spring IOC 容器中获取指定接口 Spring Bean 集合
        Map<String, MerchantAdminAbstractChainHandler> chainFilterMap = applicationContext.getBeansOfType(MerchantAdminAbstractChainHandler.class);
        chainFilterMap.forEach((beanName, bean) -> {
            // 判断 Mark 是否已经存在抽象责任链容器中，如果已经存在直接向集合新增；如果不存在，创建 Mark 和对应的集合
            List<MerchantAdminAbstractChainHandler> abstractChainHandlers = abstractChainHandlerContainer.getOrDefault(bean.mark(), new ArrayList<>());
            abstractChainHandlers.add(bean);
            abstractChainHandlerContainer.put(bean.mark(), abstractChainHandlers);
        });
        abstractChainHandlerContainer.forEach((mark, unsortedChainHandlers) -> {
            // 对每个 Mark 对应的责任链实现类集合进行排序，优先级小的在前
            unsortedChainHandlers.sort(Comparator.comparing(Ordered::getOrder));
        });
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
