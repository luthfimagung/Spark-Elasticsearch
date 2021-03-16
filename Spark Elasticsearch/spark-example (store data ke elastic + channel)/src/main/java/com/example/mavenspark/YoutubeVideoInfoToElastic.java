package com.example.mavenspark;

import com.example.mavenspark.service.SparkService;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.TaskCompletionListenerException;
import org.elasticsearch.hadoop.rest.EsHadoopNoNodesLeftException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class YoutubeVideoInfoToElastic {

    public static void main(String[] args) throws IOException {

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
            .set("es.index.auto.create", "true")        // Create Elasticsearch Index automatically when writing data.
            .set("es.nodes.wan.only", "true")           // Used if the connector is used against an Elasticsearch instance in a cloud/restricted.
            .set("es.nodes", "192.168.20.245:9200")
            .set("es.batch.size.bytes", "300000000")
            .set("es.http.timeout", "5s")
            .set("es.http.retries", "1");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> videos = sparkContext.textFile("C:/Users/Luthfi M Agung/archive/KRvideos2.csv");

        Iterator<String> id = SparkService.extractIterator(SparkService.extractVideoId(videos));
        Iterator<String> title = SparkService.extractIterator(SparkService.extractTitle(videos));
        Iterator<String> channel = SparkService.extractIterator(SparkService.extractChannel(videos));

        int count = 1;
        int error = 0;
        
        String _id = id.next();
        String _title = title.next();
        String _channel = channel.next();

        while (id.hasNext() && title.hasNext() && channel.hasNext()){
            try {
                _id = id.next();
                System.out.println("next id");
                if(!(_id.startsWith("\\n"))){
                    if(_id.equals("#NAME?") || _id.length() == 11){
                        if(!_id.contains("[]")){
                            _title = title.next();
                            _channel = channel.next();
                            System.out.println("success check id. next title & channel");
                            SparkService.exportVideoInfo(_id, _title, _channel, sparkContext, count);
                            count += 1;
                        }else
                            System.out.println("fail check id. the id contains whitespace.");
                            continue;
                    } else {
                        System.out.println("fail check id. the id is not '#NAME?' or id length is not 11.");
                        continue;
                    }
                } else {
                    System.out.println("fail check id. the id startsWith backslash.");
                    continue;
                }       
            } catch (TaskCompletionListenerException e) {
                error += 1;
            } catch (EsHadoopNoNodesLeftException e) {
                error += 1;
            } catch (Exception e) {
                error += 1;
            }
        }

        System.out.println("Successful Exported Video Info: " + count);
        System.out.println("Failed Exported Video Info: " + error);

    }

}
