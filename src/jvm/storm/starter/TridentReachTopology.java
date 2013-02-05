package storm.starter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TridentReachTopology {

	public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {
		{
			put("foo.com/blog/1",
					Arrays.asList("sally", "bob", "tim", "george", "nathan"));
			put("engineering.twitter.com/blog/5",
					Arrays.asList("adam", "david", "sally", "nathan"));
			put("tech.backtype.com/blog/123",
					Arrays.asList("tim", "mike", "john"));
		}
	};

	public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {
		{
			put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim",
					"chris", "jai"));
			put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david",
					"vivian"));
			put("tim", Arrays.asList("alex"));
			put("nathan", Arrays.asList("sally", "bob", "adam", "harry",
					"chris", "vivian", "emily", "jordan"));
			put("adam", Arrays.asList("david", "carissa"));
			put("mike", Arrays.asList("john", "bob"));
			put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
		}
	};
	
	public static class GetTweeters extends BaseBasicBolt{
		
		//change this value when backward-incompatibility is needed!
		private static final long serialVersionUID = 7884982521910033258L;

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			//this id is a request id, all subsequent tuple must contains request id
			Object id = input.getValue(0);
			String url = input.getString(1);
			
			List<String> tweeters = TWEETERS_DB.get(url);
			if(tweeters != null){
				for (String tweeter: tweeters) {
					collector.emit(new Values(id, tweeter));
				}
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "tweeter"));
		}
	}
	
	public static class GetFollowers extends BaseBasicBolt{

		//change this value when backward-incompatibility is needed!
		private static final long serialVersionUID = -21413151551265565L;

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			Object id = input.getValue(0);
			String tweeter = input.getString(1);
			
			List<String> followers = FOLLOWERS_DB.get(tweeter);
			if(followers != null){
				for(String follower: followers){
					collector.emit(new Values(id, follower));
				}
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "follower"));
		}
		
	}
	
	public static class PartialUniquer extends BaseBatchBolt{

		private static final long serialVersionUID = -8620597452774568816L;
		
		private Object _id;
		private BatchOutputCollector _collector;
		private Set<String> _followerSet;
		
		@Override
		public void prepare(Map conf, TopologyContext context,
				BatchOutputCollector collector, Object id) {
			_id = id;
			_collector = collector;
			_followerSet = new HashSet<String>();
		}

		@Override
		public void execute(Tuple tuple) {
			_followerSet.add(tuple.getString(1));
		}

		@Override
		public void finishBatch() {
			_collector.emit(new Values(_id, _followerSet.size()));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "partial-count"));
		}
	}
	
	public static class CountAggregator extends BaseBatchBolt{

		private static final long serialVersionUID = -7632790693714309282L;
		
		private Object _id;
		private BatchOutputCollector _collector;
		private int _totalCount;
		
		@Override
		public void prepare(Map conf, TopologyContext context,
				BatchOutputCollector collector, Object id) {
			_id = id;
			_collector = collector;
		}

		@Override
		public void execute(Tuple tuple) {
			_totalCount += tuple.getInteger(1);
		}

		@Override
		public void finishBatch() {
			_collector.emit(new Values(_id, _totalCount));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "count"));	
		}
	}
	

	/**
	 * @param args
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 */
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("trident-reach");
		builder.addBolt(new GetTweeters(), 4);
		builder.addBolt(new GetFollowers(), 12).shuffleGrouping();
		builder.addBolt(new PartialUniquer(), 6).fieldsGrouping(new Fields("id", "follower"));
		builder.addBolt(new CountAggregator(), 3).fieldsGrouping(new Fields("id"));
		
		
		Config conf = new Config();
		
		if(args == null || args.length == 0){
			conf.setMaxTaskParallelism(3);
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("reach-drpc", conf, builder.createLocalTopology(drpc));
			
            String[] urlsToTry = new String[] { "foo.com/blog/1", "engineering.twitter.com/blog/5", "notaurl.com"};
            for(String url: urlsToTry) {
                System.out.println("Reach of " + url + ": " + drpc.execute("trident-reach", url));
            }
            
            cluster.shutdown();
            drpc.shutdown();
		} else {
			conf.setNumWorkers(6);
			StormSubmitter.submitTopology(args[0],conf, builder.createRemoteTopology());
		}
		
	}

}
