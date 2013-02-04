package storm.starter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
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
			
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
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
			// TODO Auto-generated method stub
			
		}
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}
