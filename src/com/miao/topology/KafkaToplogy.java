package com.miao.topology;

import com.miao.bout.WordOut;
import com.miao.bout.WordSplit;
import com.miao.spout.MessageSchem;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class KafkaToplogy {
	public static void main(String[] args) {
		String topic = "myBaby2";
		String zkRoot = "/kafka-storm";
		String spoutId ="kafkaspout";
		BrokerHosts brokerHosts = new ZkHosts("MiaoMiao01:2181,MiaoMiao02:2181,MiaoMiao03:2181");
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
		spoutConfig.forceFromStart = true; 
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageSchem());
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(spoutId, new KafkaSpout(spoutConfig));
		builder.setBolt("word_split", new WordSplit(),4).shuffleGrouping(spoutId);
		builder.setBolt("word_out", new WordOut(),5).fieldsGrouping("word_split", new Fields("word_ok"));
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(4);
		conf.setNumAckers(0);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topic, conf, builder.createTopology());
	}

}
