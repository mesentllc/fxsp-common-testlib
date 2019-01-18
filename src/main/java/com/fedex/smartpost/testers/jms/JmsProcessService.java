package com.fedex.smartpost.testers.jms;

import com.fedex.smartpost.common.exception.RecoverableException;
import com.fedex.smartpost.common.exception.UnrecoverableException;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jms.connection.UserCredentialsConnectionFactoryAdapter;
import org.springframework.jms.core.JmsTemplate;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.naming.NamingException;

public class JmsProcessService {
	private static final Log LOGGER = LogFactory.getLog(JmsProcessService.class);
	private final JmsUtils jmsUtils;
	private UserCredentialsConnectionFactoryAdapter cfAdapter;
	private JmsTemplate jmsTemplate;
	private MessageModel model;

	private void getMessageFromJms() {
		jmsTemplate.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
		model = null;
		// Need to do it this way, (Creating a MessageConsumer attaching it to the session, if you want it to acknowledge
		// the messages -- otherwise it just reads the first message and keeps it on the JMS queue.
		// Wait for 100 milliseconds (max) to see if there are any messages
		jmsTemplate.execute(session -> {
			MessageConsumer consumer = session.createConsumer(
					jmsTemplate.getDestinationResolver().resolveDestinationName(session, jmsTemplate.getDefaultDestinationName(),
					                                                            jmsTemplate.isPubSubDomain()));
			try {
				Message message = consumer.receive(100);
				if (message != null) {
					model = new MessageModel(message);
					message.acknowledge();
					return true;
				}
			}
			catch (Exception e) {
				LOGGER.error("Error attempting to received JMS message:", e);
				return false;
			}
			finally {
				consumer.close();
			}
			return true;
		}, true);
	}

	public JmsProcessService(String url) throws NamingException {
		if (url == null) {
			throw new UnrecoverableException("LDAP URL MUST NOT be NULL!", false);
		}
		jmsUtils = new JmsUtils(url);
	}

	public void setCF(String cfName, String username, String password) throws NamingException {
		if (cfName == null) {
			throw new UnrecoverableException("Connection Factory Name MUST NOT be NULL!", false);
		}
		LOGGER.debug(String.format("Initializing the Connection Factory - %s - [%s/%s]", cfName, username, password));
		if (!cfName.startsWith("fxClientUID=")) {
			cfAdapter = jmsUtils.getConnectionFactory("fxClientUID=" + cfName, username, password);

		}
		else {
			cfAdapter = jmsUtils.getConnectionFactory(cfName, username, password);
		}
		LOGGER.debug(String.format("Connection Factory - %s - created", cfName));
	}

	public void setJmsTemplate(String name, boolean isTopic) {
		if (name == null) {
			throw new UnrecoverableException("Destination Name MUST NOT be NULL!", false);
		}
		LOGGER.debug(String.format("Setting up %s - %s", isTopic ? "Topic" : "Queue", name));
		if (cfAdapter == null) {
			throw new UnrecoverableException("Must setup the connection factory first.", false);
		}
		if (!name.startsWith("fxClientDestinationUID=")) {
			jmsTemplate = jmsUtils.getJmsTemplate(cfAdapter, "fxClientDestinationUID=" + name, isTopic);
		}
		else {
			jmsTemplate = jmsUtils.getJmsTemplate(cfAdapter, name, isTopic);
		}
		LOGGER.debug(name + " created");
	}

	public MessageModel consume() {
		if (jmsTemplate == null) {
			throw new UnrecoverableException("Must setup the JmsTemplate first.", false);
		}
		getMessageFromJms();
		return model;
	}

	public void publish(MessageModel model) {
		if (jmsTemplate == null) {
			throw new UnrecoverableException("Must setup the JmsTemplate first.", false);
		}
		if (model == null) {
			throw new RecoverableException("Nothing to send - skipping the publish.", false);
		}
		LOGGER.debug("Publishing: " + ReflectionToStringBuilder.toString(model, ToStringStyle.MULTI_LINE_STYLE));
		jmsTemplate.send(new JmsMessageCreator(model.getMessageText(), model.getProperties()));
		LOGGER.debug("Message Sent");
	}
}
