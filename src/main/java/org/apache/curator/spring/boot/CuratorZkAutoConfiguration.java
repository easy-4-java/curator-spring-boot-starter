package org.apache.curator.spring.boot;

import java.util.ArrayList;
import java.util.stream.Collectors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;

@Configuration
@ConditionalOnClass({CuratorFramework.class, RetryPolicy.class, InterProcessLock.class, ZooKeeper.class})
@EnableConfigurationProperties(CuratorZkProperties.class)
public class CuratorZkAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean
	public RetryPolicy retryPolicy(CuratorZkProperties properties) {
		// 1 重试策略：初试时间为1s 重试10次
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(properties.getBaseSleepTimeMs(), properties.getMaxRetries(), properties.getMaxSleepMs());
		return retryPolicy;
	}
	
	@Bean
	@ConditionalOnMissingBean
	public CuratorFramework curatorClient(CuratorZkProperties properties, RetryPolicy retryPolicy) {

		// 1、创建连接实例
		CuratorFramework curatorClient = CuratorFrameworkFactory.builder()
				.connectString(properties.getConnectString())
				.connectionTimeoutMs(properties.getConnectionTimeoutMs())
				.canBeReadOnly(properties.isCanBeReadOnly())
				.authorization(CollectionUtils.isEmpty(properties.getAuthInfo()) ? new ArrayList<AuthInfo>()
						: properties.getAuthInfo().stream().map(info -> {
							return new AuthInfo(info.getScheme(), info.getAuth().getBytes());
						}).collect(Collectors.toList()))
				.ensembleTracker(properties.isWithEnsembleTracker())
				.namespace(properties.getNamespace())
				.retryPolicy(retryPolicy)
				.sessionTimeoutMs(properties.getSessionTimeoutMs())
				.build();
		// 2、开启连接
		curatorClient.start();

		System.out.println(States.CONNECTED);
		System.out.println(curatorClient.getState());

		return curatorClient;
	}

	@Bean
	public CuratorZkTemplate curatorZkTemplate(CuratorZkProperties properties, CuratorFramework curatorClient, RetryPolicy retryPolicy) {
		return new CuratorZkTemplate(curatorClient, retryPolicy, properties.getSessionTimeoutMs());
	}
	
}
