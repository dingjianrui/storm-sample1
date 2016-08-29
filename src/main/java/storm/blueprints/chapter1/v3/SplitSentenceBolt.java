package storm.blueprints.chapter1.v3;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt {
	private OutputCollector collector;
	
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector = arg2;
	}
	
	@Override
	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		String sentence = arg0.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for(String word : words)
		{
			this.collector.emit(new Values(word));
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("word"));
	}
}
