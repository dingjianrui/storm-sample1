package storm.blueprints.chapter1.v3;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

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
		this.collector.ack(arg0);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("word"));
	}
}
