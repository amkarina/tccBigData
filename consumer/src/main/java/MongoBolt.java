import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.PrintWriter;
import java.util.Map;

import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;
import org.bson.Document;
import org.json.JSONObject;


public class MongoBolt extends BaseBasicBolt {

    MongoClient mongoClient;
    MongoDatabase database;
    MongoCollection<Document> collection;

    public void prepare(Map stormConf, TopologyContext context) {

        this.mongoClient = new MongoClient( "localhost" , 27017 );
        this.database = this.mongoClient.getDatabase("tcc");
        this.collection = this.database.getCollection("recommendations");

    }


    public void execute(Tuple input,BasicOutputCollector collector) {

        String userid = input.getValue(0).toString();
        System.out.println(userid);
        for (Document cur : this.collection.find(eq("userId", Integer.parseInt(userid)))) {

            //System.out.println(cur.toJson());
            JSONObject recomendacoes = new JSONObject(cur.toJson());
            String recomendacao = recomendacoes.get("movieId").toString();
            System.out.println("Recomendacoes: " + recomendacao);
            collector.emit(new Values(userid, recomendacao));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("userid", "recomendacoes"));
    }

    public void cleanup() {}

}
