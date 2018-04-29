import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class Pratica3Main {

    public static void main(String[] args) throws InterruptedException {

        //Buscar os ids dos usuarios do no Kafka e recuperar as recomendacoes no mongoDB

        //Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("KafkaSpout", new KafkaSpout());
        builder.setBolt("MongoBolt", new MongoBolt()).shuffleGrouping("KafkaSpout");

        //Configuration
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 2048);
        conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, false);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
        conf.put(Config.STORM_ZOOKEEPER_PORT, 2185);

        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("Pratica3MainTopology", conf, builder.createTopology());
            Thread.sleep(500000);
        }
        finally {
            System.out.println("Shutting Down Cluster!");
            cluster.shutdown();
        }
    }
}
