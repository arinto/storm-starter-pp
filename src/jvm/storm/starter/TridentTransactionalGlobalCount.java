package storm.starter;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MemoryTransactionalSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TridentTransactionalGlobalCount {
	public static final int PARTITION_TAKE_PER_BATCH = 3;

	@SuppressWarnings("serial")
	public static final Map<Integer, List<List<Object>>> DATA = new HashMap<Integer, List<List<Object>>>() {
		{
			put(0, new Vector<List<Object>>() {
				{
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("chicken"));
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("apple"));
				}
			});
			put(1, new Vector<List<Object>>() {
				{
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("apple"));
					add(new Values("banana"));
				}
			});
			put(2, new Vector<List<Object>>() {
				{
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("dog"));
					add(new Values("dog"));
					add(new Values("dog"));
				}
			});
		}
	};

	public static class Value {
		int count = 0;
		BigInteger txid;
	}
	
	public static Map<String, Value> DATABASE = new HashMap<String, Value>();
	public static final String GLOBAL_COUNT_KEY = "GLOBAL-COUNT";
	
	public static class BatchCount extends BaseBatchBolt{
		Object _id;
		BatchOutputCollector _collector;
		
		int _count = 0;
		
		@Override
		public void prepare(Map conf, TopologyContext context,
				BatchOutputCollector collector, Object id) {
			_id = id;
			_collector = collector;
		}

		@Override
		public void execute(Tuple tuple) {
			_count++;
			
		}

		@Override
		public void finishBatch() {
			_collector.emit(new Values(_id, _count));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "count"));
		}	
	}
	
	public static class UpdateGlobalCount extends BaseTransactionalBolt implements ICommitter{
		TransactionAttempt _attempt;
		BatchOutputCollector _collector;
		
		int _sum = 0;
		@Override
		public void prepare(Map conf, TopologyContext context,
				BatchOutputCollector collector, TransactionAttempt attempt) {
			_attempt = attempt;
			_collector = collector;
		}

		@Override
		public void execute(Tuple tuple) {
			_sum += tuple.getInteger(1);
		}

		@Override
		public void finishBatch() {
			Value val = DATABASE.get(GLOBAL_COUNT_KEY);
			Value newVal;
			
			//Question on this condition, what we receive stale transaction id? 
			//Should we only update value when attempt transaction id is greater than value transaction id?
			//Possible answer: the order of updating newVal is not important
			//since our councern is on obtaining global count
			if(val == null || !val.txid.equals(_attempt.getTransactionId())){
				newVal = new Value();
				newVal.txid = _attempt.getTransactionId();
				if(val == null){
					newVal.count = _sum;
				}else{
					newVal.count = val.count + _sum;
				}
				DATABASE.put(GLOBAL_COUNT_KEY, newVal);
			}else{
				newVal = val;
			}		
			_collector.emit(new Values(_attempt, newVal.count));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "sum"));
		}
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final String topoId = "global-count-topo";
		final String memTransSpoutId = "mem-trans-spout";
		final String partialCountBoltId = "partial-count-bolt";
		final String sumBoltId = "sum-bolt";
		
		MemoryTransactionalSpout spout = new MemoryTransactionalSpout(DATA, new Fields ("word"), PARTITION_TAKE_PER_BATCH);
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder(topoId, memTransSpoutId, spout, 3);
		builder.setBolt(partialCountBoltId, new BatchCount(), 5).noneGrouping(memTransSpoutId);
		builder.setBolt(sumBoltId, new UpdateGlobalCount()).allGrouping(partialCountBoltId);
		
		LocalCluster cluster = new LocalCluster();
		
		Config config = new Config();
		config.setDebug(true);
		config.setMaxSpoutPending(3);
		
		final String globalCountTopoId = "global-count-topology";
		cluster.submitTopology(globalCountTopoId, config, builder.buildTopology());
		
		Utils.sleep(3000);
		cluster.shutdown();
	}

}
