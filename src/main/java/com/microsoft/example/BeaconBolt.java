package com.microsoft.example;

import java.util.HashMap;

import scala.Option;
import scala.collection.Map;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BeaconBolt extends BaseBasicBolt {

	HashMap<String, Integer> counts = new HashMap<String,Integer>();
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String uuid = (String)input.getValue(0);
		String accuracy = (String)input.getValue(1);
		System.out.println(uuid+":"+accuracy);
		Integer count = counts.get(uuid);
		
		if (count == null)
			count = 0;
		count++;
		counts.put(uuid,count);
		
		collector.emit(new Values(uuid,count));
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("uuid","count"));
		
	}

}
