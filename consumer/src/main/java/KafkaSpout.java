import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.consumer.*;
import java.util.Properties;

public class KafkaSpout extends BaseRichSpout{

    private SpoutOutputCollector collector;
    private Properties props;
    private KafkaConsumer<String,String> consumer;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        this.collector = collector;

        //Configuracao
        this.props = new Properties();
        this.props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        this.props.put(ConsumerConfig.GROUP_ID_CONFIG, "userId");
        this.props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        this.props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500");
        this.props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        this.props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //this.props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "range");

        this.consumer = new KafkaConsumer<String, String>(this.props);

        // Subscribe to the topic.
        this.consumer.subscribe(Arrays.asList("getMoviesForUser"));


    }

    public void nextTuple() {
        try
        {
            boolean continuar = true;

            while (continuar) {
                ConsumerRecords<String, String> records = this.consumer.poll(100);

                    if (records.isEmpty()) {
                        //System.out.println("Nenhum registro");
                        continuar = false;
                    }

                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                        String userid = record.value();
                        collector.emit(new Values(userid));
                        Thread.sleep(1000);
                    }


            }
        }

        catch(Exception e) {}
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("userId"));
    }

}