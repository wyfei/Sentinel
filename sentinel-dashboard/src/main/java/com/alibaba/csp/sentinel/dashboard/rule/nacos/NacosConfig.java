package com.alibaba.csp.sentinel.dashboard.rule.nacos;

import com.alibaba.csp.sentinel.dashboard.datasource.entity.rule.DegradeRuleEntity;
import com.alibaba.csp.sentinel.dashboard.datasource.entity.rule.FlowRuleEntity;
import com.alibaba.csp.sentinel.datasource.Converter;
import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import java.util.List;
import java.util.Properties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author: wangyufei
 * @Date: 2022/9/23 14:29
 * @Description:
 */
@Configuration
public class NacosConfig {

	@Value("${nacos.address:''}")
	private String serverAddr;

	@Value("${nacos.namespace:''}")
	private String namespace;

	@Bean
	public Converter<List<FlowRuleEntity>, String> flowRuleEntityEncoder() {
		return JSON::toJSONString;
	}

	@Bean
	public Converter<String, List<FlowRuleEntity>> flowRuleEntityDecoder() {
		return s -> JSON.parseArray(s, FlowRuleEntity.class);
	}

	@Bean
	public Converter<List<DegradeRuleEntity>, String> DegradeRuleEntityEntityEncoder() {
		return JSON::toJSONString;
	}

	@Bean
	public Converter<String, List<DegradeRuleEntity>> DegradeRuleEntityEntityDecoder() {
		return s -> JSON.parseArray(s, DegradeRuleEntity.class);
	}

	@Bean
	public ConfigService nacosConfigService() throws NacosException {
		Properties properties = new Properties();
		properties.put(PropertyKeyConst.SERVER_ADDR, serverAddr);
		properties.put(PropertyKeyConst.NAMESPACE, namespace);
		return ConfigFactory.createConfigService(properties);
	}

}
