package com.fedex.smartpost.testers.jms;

import org.joda.time.DateTime;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class MessageModel {
	private final Map<String, Object> properties;
	private final Map<String, Object> jmsHeaderFields;
	private final String messageText;

	private static Map<String, Object> getJMSHeaders(Message message) throws JMSException {
		Map<String, Object> headers = new HashMap<>();
		headers.put("JMSCorrelationID", message.getJMSCorrelationID());
		headers.put("JMSDestination", message.getJMSDestination());
		headers.put("JMSExpiration", message.getJMSExpiration());
		headers.put("JMSMessageID", message.getJMSMessageID());
		headers.put("JMSTimestamp", new DateTime(message.getJMSTimestamp()));
		return headers;
	}

	private HashMap<String, Object> getMessageProperties(Message message) throws JMSException {
		HashMap<String, Object> properties = new HashMap<>();
		Enumeration enumeration = message.getPropertyNames();
		while (enumeration.hasMoreElements()) {
			String propertyName = (String) enumeration.nextElement();
			properties.put(propertyName, message.getObjectProperty(propertyName));
		}
		return properties;
	}

	private String convertBytesMessageToText(BytesMessage bytesMessage) {
		try {
			byte[] je = new byte[(int) bytesMessage.getBodyLength()];
			bytesMessage.readBytes(je);
			return new String(je);
		}
		catch (JMSException var2) {
			return null;
		}
	}

	public MessageModel(Message message) throws JMSException {
		if (message == null) {
			throw new JMSException("No message was sent to MessageModel.");
		}
		jmsHeaderFields = getJMSHeaders(message);
		properties = getMessageProperties(message);
		messageText = convertBytesMessageToText((BytesMessage) message);
	}

	public MessageModel(File file, Map<String, Object> properties) throws IOException {
		this.jmsHeaderFields = null;
		this.properties = properties;
		BufferedReader br = new BufferedReader(new FileReader(file));
		StringBuilder sb = new StringBuilder();
		while (br.ready()) {
			sb.append(br.readLine()).append("\n");
		}
		this.messageText = sb.toString();
	}

	public Map<String, Object> getJmsHeaderFields() {
		return jmsHeaderFields;
	}

	public Map<String, Object> getProperties() {
		return properties;
	}

	public String getMessageText() {
		return messageText;
	}
}
