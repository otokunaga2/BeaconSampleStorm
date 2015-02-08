package com.microsoft.example;

import java.util.Map;
import java.util.Random;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import scala.collection.immutable.HashMap;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class BeaconSpout extends BaseRichSpout {
	private Jedis jedis;
	private SpoutOutputCollector _collector;
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		String content = jedis.rpop("navigation");
		if(content == null || "nil".equals(content)){
			try{
				Thread.sleep(300);
			}catch(InterruptedException e){
				
			}
		}else{
			JSONObject obj = (JSONObject)JSONValue.parse(content);
			String uuid = obj.get("uuid").toString();
			String accuracy = obj.get("proximity").toString();
//			String product = obj.get("product").toString();
//			String type = obj.get("type").toString();
//			HashMap<String,String> map = new HashMap<String, String>();
			_collector.emit(new Values(uuid));
		}
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector _collector) {
		  //Set the instance collector to the one passed in
	    this._collector = _collector;
	    //create Jedis object
	    this.jedis = new Jedis("192.168.100.106", 6379);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("otherdata"));
	}

}
