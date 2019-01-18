package com.fedex.smartpost.testers.jms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jms.connection.UserCredentialsConnectionFactoryAdapter;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.destination.JndiDestinationResolver;
import org.springframework.jndi.JndiTemplate;

import javax.jms.ConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Hashtable;

public class JmsUtils {
	private static final Log LOGGER = LogFactory.getLog(JmsUtils.class);
	private static final String FEDEX_DECORATOR_INITIAL_CTX = "com.fedex.mi.decorator.jms.FedexTibcoInitialContext";
	private final InitialContext initialContext;

	public JmsUtils(String url) throws NamingException {
		LOGGER.info("Initializing initialContext...");
		Hashtable<String, Object> initialContextMap = new Hashtable<>();
		initialContextMap.put("java.naming.provider.url", url);
		initialContextMap.put("java.naming.factory.initial", FEDEX_DECORATOR_INITIAL_CTX);
		LOGGER.info(String.format("createInitialContextMap =====> %s", initialContextMap.toString()));
		initialContext = new InitialContext(initialContextMap);
	}

	private ConnectionFactory getConnectionFactory(String name) throws NamingException {
		LOGGER.trace(String.format("Attempting to lookup Connection Factory named %s", name));
		return (ConnectionFactory) initialContext.lookup(name);
	}

	private JndiDestinationResolver getDestinationResolver(boolean fallbackToDynamicDestination) {
		LOGGER.trace(String.format("Attempting to create a JNDI DestinationResolver - %sfallback to Dynamic Destination.", (fallbackToDynamicDestination ? "" : "DO NOT ")));
		JndiDestinationResolver jndiDestinationResolver = new JndiDestinationResolver();
		jndiDestinationResolver.setJndiTemplate(new JndiTemplate() {
			@Override
			protected Context createInitialContext() {
				return initialContext;
			}
		});
		jndiDestinationResolver.setFallbackToDynamicDestination(fallbackToDynamicDestination);
		LOGGER.trace("DestinationResolver Created.");
		return jndiDestinationResolver;
	}

	public UserCredentialsConnectionFactoryAdapter getConnectionFactory(String name, String username, String pwd) throws NamingException {
		LOGGER.trace(String.format("Creating UserCredentialsConnectionFactoryAdapter for %s", name));
		UserCredentialsConnectionFactoryAdapter adapter = new UserCredentialsConnectionFactoryAdapter();
		adapter.setTargetConnectionFactory(getConnectionFactory(name));
		adapter.setUsername(username);
		adapter.setPassword(pwd);
		LOGGER.trace(String.format("Created UserCredentialsConnectionFactoryAdapter for %s, with credentials of %s/%s", name, username, pwd));
		return adapter;
	}

	public JmsTemplate getJmsTemplate(UserCredentialsConnectionFactoryAdapter adapter, String topicName, boolean isTopic) {
		LOGGER.trace("getJmsTemplate initializing...");

		LOGGER.trace(String.format("Attempting to build JMS Template for %s", topicName));
		JmsTemplate jmsTemplate = new JmsTemplate();
		LOGGER.trace(String.format("Attempting to set destination for %s", topicName));
		jmsTemplate.setConnectionFactory(adapter);
		jmsTemplate.setDefaultDestinationName(topicName);
		jmsTemplate.setPubSubDomain(isTopic);
		jmsTemplate.setDestinationResolver(getDestinationResolver(true));
		jmsTemplate.afterPropertiesSet();
		LOGGER.trace(String.format("Completed JMS Template for %s", topicName));
		return jmsTemplate;
	}
}
