package com.fedex.smartpost.testers.jms;

import com.fedex.smartpost.common.exception.UnrecoverableException;
import org.springframework.jms.core.MessageCreator;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

public class JmsMessageCreator implements MessageCreator {
	private String message = "";
	private Properties messageProperties = new Properties();

	public String getMessage() {
		return message;
	}

	public Properties getMessageProperties() {
		return messageProperties;
	}

	private Properties convertToProperties(Map<String, Object> messageProperties) {
		Properties properties = new Properties();
		messageProperties.keySet().forEach((key) -> {
			properties.put(key, messageProperties.get(key));
		});
		return properties;
	}

	public JmsMessageCreator(String message, Map<String, Object> messageProperties) {
		if (message != null) {
			this.message = message;
		}
		if (messageProperties != null) {
			this.messageProperties = convertToProperties(messageProperties);
		}
	}

	@Override
	public Message createMessage(Session session) throws JMSException {
		if (session == null) {
			throw new UnrecoverableException("JMS Session NULL - Please review immediately.", true);
		}
		BytesMessage bytesMessage = session.createBytesMessage();

		bytesMessage.writeBytes(message.getBytes());
		Enumeration<Object> enumeration = messageProperties.keys();
		while (enumeration.hasMoreElements()) {
			String key = (String) enumeration.nextElement();
			bytesMessage.setStringProperty(key, (String) messageProperties.get(key));
		}
		return bytesMessage;
	}
}
