package com.asirtech.producers;


import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerializer<T> implements Serializer<T> {

	private final ObjectMapper objectMapper = new ObjectMapper();
	public JsonSerializer() {}
	public void configure(Map<String,?> config,boolean isKey) {}
	
	public byte[] serialize(String topic, T data) {
		if(data==null) {
			return null;
		}
		try {
			return objectMapper.writeValueAsBytes(data);
		}catch(Exception e) {
			throw new SerializationException("Error serializing JSON message",e);
		}
	}

}
