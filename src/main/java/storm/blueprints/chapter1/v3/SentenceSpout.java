package storm.blueprints.chapter1.v3;

import java.util.Map;
import storm.blueprints.utils.Utils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SentenceSpout extends BaseRichSpout {
	
	private SpoutOutputCollector collector;
	private int index = 0;
	private String[] sentences = {
			"my dog has fleas",
			"i like cold beverages",
			"the dog ate my homework",
			"don't have a cow man",
			"i don't think i like fleas"
	};

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("sentence"));
	}
	
	@Override
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}
	
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		this.collector.emit(new Values(sentences[index]));
		index++;
		if(index >= sentences.length)
		{
			index = 0;
		}
		Utils.waitForMillis(1);
	}
}