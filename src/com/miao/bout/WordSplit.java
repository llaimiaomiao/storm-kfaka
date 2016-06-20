package com.miao.bout;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordSplit extends BaseBasicBolt {

	
	private static final long serialVersionUID = 623572547L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String result = tuple.getString(0);
		String [] words = result.split(" ");
		for(String word :words){
			if(StringUtils.isNotBlank(word)){
				collector.emit(new Values(word));
			}
			
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declare) {
		declare.declare(new Fields("word_ok"));
		
	}

}
