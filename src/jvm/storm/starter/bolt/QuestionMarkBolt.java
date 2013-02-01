package storm.starter.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class QuestionMarkBolt extends BaseRichBolt{

	private OutputCollector _collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		
	}

	//just a simple bolt, add three question-mark-symbols in every tuple
	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getString(0);
		_collector.emit(tuple, new Values(word + "???"));
		_collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}
