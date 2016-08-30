package storm.blueprints.chapter1.v3;

import java.util.Map;
import java.util.UUID;

import storm.blueprints.utils.Utils;

import org.apache.storm.shade.org.jboss.netty.util.internal.ConcurrentHashMap;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SentenceSpout extends BaseRichSpout {
	
	private SpoutOutputCollector collector;
	private ConcurrentHashMap<UUID, Values> pending;
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
		pending = new ConcurrentHashMap<UUID,Values>();
	}
	
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Values values = new Values(sentences[index]);
		UUID msgId = UUID.randomUUID();
		this.collector.emit(values, msgId);
		pending.put(msgId, values);
		index++;
		if(index >= sentences.length)
		{
			index = 0;
			//index++;
		}
		Utils.waitForMillis(1);
	}
	
	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		this.pending.remove(msgId);
	}
	
	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		this.collector.emit(this.pending.get(msgId),msgId);
	}
}