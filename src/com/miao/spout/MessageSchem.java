package com.miao.spout;

import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MessageSchem implements Scheme{

	private static final long serialVersionUID = 346346312L;

	@Override
	public List<Object> deserialize(byte[] bytes) {
		try {
			String msg = new String(bytes, "utf-8");
			return new Values(msg);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("msg");
	}

}
