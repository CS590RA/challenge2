package main.java.netks;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

public class Netmain 
{
private final Logger LOGGER = Logger.getLogger(this.getClass());
  private static final String KAFKA_TOPIC ="new";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
        BasicConfigurator.configure();

        if (args != null && args.length > 0)
        {
            try {
				StormSubmitter.submitTopology(
				    args[0],
				    createConfig(false),
				    createTopology());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        else
        {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(
                "network-analysis",
                createConfig(true),
                createTopology());
            try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            cluster.shutdown();
        }
    }

    private static StormTopology createTopology()
    {
        SpoutConfig kafkaConf = new SpoutConfig(
            new ZkHosts("localhost:2181"),
            KAFKA_TOPIC,
            "/kafka",
            "KafkaSpout");
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
       TopologyBuilder topology = new TopologyBuilder();

    //    topology.setSpout("k_spout", new Kspout(), 4);
       topology.setSpout("kafka_spout", new KafkaSpout(kafkaConf), 4);
       topology.setBolt("Training_Bolt", new TrainingBolt(), 4)
       .shuffleGrouping("kafka_spout");

//        topology.setBolt("Bandwidth_Bolt", new BandwidthBolt(), 4)
  //              .shuffleGrouping("kafka_spout");
       // topology.setBolt("CounterBolt", new CounterBolt(), 4)
        //.shuffleGrouping("Bandwidth_Bolt");
       /* topology.setBolt("TopBolt", new TopBolt(), 4)
        .shuffleGrouping("CounterBolt");
        topology.setBolt("PrintBolt", new PrintBolt(), 4)
                .shuffleGrouping("TopBolt");*/

        return topology.createTopology();
    }

    private static Config createConfig(boolean local)
    {
        int workers = 1;
        Config conf = new Config();
        conf.setDebug(true);
        if (local)
            conf.setMaxTaskParallelism(workers);
        else
            conf.setNumWorkers(workers);
        return conf;
    }


	}


