package com.alibaba.csp.sentinel.dashboard.rule.nacos.flow;

import com.alibaba.csp.sentinel.dashboard.datasource.entity.rule.FlowRuleEntity;
import com.alibaba.csp.sentinel.dashboard.rule.DynamicRulePublisher;
import com.alibaba.csp.sentinel.dashboard.rule.nacos.NacosConfigUtil;
import com.alibaba.csp.sentinel.datasource.Converter;
import com.alibaba.csp.sentinel.util.AssertUtil;
import com.alibaba.nacos.api.config.ConfigService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author: wangyufei
 * @Date: 2022/9/23 14:37
 * @Description:
 */
@Component("flowRuleNacosPublisher")
public class FlowRuleNacosPublisher implements DynamicRulePublisher<List<FlowRuleEntity>> {

	@Autowired
	private ConfigService configService;
	@Autowired
	private Converter<List<FlowRuleEntity>, String> converter;

	@Override
	public void publish(String app, List<FlowRuleEntity> rules) throws Exception {
		AssertUtil.notEmpty(app, "app name cannot be empty");
		if (rules == null) {
			return;
		}
		configService.publishConfig(app + NacosConfigUtil.FLOW_DATA_ID_POSTFIX,
				NacosConfigUtil.GROUP_ID, converter.convert(rules));
	}
}
