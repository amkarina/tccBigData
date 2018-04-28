import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

import org.apache.kafka.clients.consumer.*;
import java.util.Properties;

public class KafkaSpout extends BaseRichSpout{

    private SpoutOutputCollector collector;
    private Properties props;
    private KafkaConsumer<String, String> consumer;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        this.collector = collector;

        //Configuracao
        this.props = new Properties();
        this.props.put("bootstrap.servers", "localhost:9092");
        this.props.put("group.id", "userId");
        this.props.put("enable.auto.commit", "true");
        this.props.put("auto.commit.interval.ms", "1000");
        this.props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.props.put("partition.assignment.strategy", "range");
        this.consumer = new KafkaConsumer<String,String>(this.props);
        this.consumer.subscribe("userId");

    }

    public void nextTuple() {

        try
        {
            boolean continuar = true;

            while (continuar) {
                for (ConsumerRecords<String, String> records : this.consumer.poll(1000).values()
                     ) {
                    if (records.records().isEmpty()) {
                        System.out.println("Nenhum registro");
                        continuar = false;
                    }

                    for (ConsumerRecord<String, String> record : records.records()) {
                        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                        String userid = record.value();
                        collector.emit(new Values(userid));
                        Thread.sleep(1000);
                    }
                }

            }
        }

        catch(Exception e) {}
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("userId"));
    }

}