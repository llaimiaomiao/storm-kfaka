package com.miao.bout;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WordOut extends BaseBasicBolt{

	
	private static final long serialVersionUID = 34654574L;
	private FileWriter fw ;
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		try {
			fw = new FileWriter("E://"+"wordcount"+UUID.randomUUID());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String word = tuple.getString(0);
		
		try {
			fw.write(word);
			fw.write("\n");
			fw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declare) {
		
	}

}
