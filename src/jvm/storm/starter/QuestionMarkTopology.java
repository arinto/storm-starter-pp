package storm.starter;

import storm.starter.bolt.QuestionMarkBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class QuestionMarkTopology {
	
	
	
	/**
	 * @param args
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 */
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		
		//define all IDs for spouts and bolts
		String sWordId = "word";
		String bQOneId = "question1";
		String bQTwoId = "question2";
		
		//topology = wordSpout -> question1Bolt -> question2Bolt
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(sWordId, new TestWordSpout(), 10);
		builder.setBolt(bQOneId, new QuestionMarkBolt(), 3)
			.shuffleGrouping(sWordId);
		builder.setBolt(bQTwoId, new QuestionMarkBolt(), 3)
			.shuffleGrouping(bQOneId);
		
		Config conf = new Config();
		conf.setDebug(true);
		
		//submit and run the topology
		if(args != null && args.length > 0){
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}else{
			String clusterName = "test";
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(clusterName, conf, builder.createTopology());
			Utils.sleep(2000);
			cluster.killTopology(clusterName);
			cluster.shutdown();
		}
	}
}
