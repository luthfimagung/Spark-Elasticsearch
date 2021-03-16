package com.example.mavenspark;

import com.example.mavenspark.service.SparkService;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class YoutubeTitleWordCountToElastic {

    public static void main(String[] args) throws IOException {

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
            .set("es.index.auto.create", "true")
            .set("es.nodes", "192.168.20.245:9200")
            // .set("es.port", "9200");
            .set("es.nodes.wan.only", "true")
            .set("es.batch.size.bytes", "100000000");
            // .set("spark.executor.memory", "500m");
            // .set("es.scroll.keepalive", "1m");
            // .set("es.batch.size.entries", "1000")
            // .set("es.scroll.size", "1")
            // .set("es.mapping.id", "id");
            // .set("es.write.operation", "upsert");
            // .set("es.batch.write.refresh", "false");
            // .set("es.batch.write.retry.count", "50");
            // .set("es.batch.write.retry.wait", "500");
            // .set("spark.es.http.timeout", "5m");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> videos = sparkContext.textFile("C:/Users/Luthfi M Agung/archive/JPvideos.csv");

        JavaRDD<String> titles = videos.map(SparkService::extractTitle).filter(StringUtils::isNotBlank);

        JavaRDD<String> eachTitle = titles
                .flatMap(title -> Arrays.asList(title.trim().replaceAll("\\p{Punct}", "")).iterator());

        // COUNTING
        Map<String, Long> titleCounts = eachTitle.countByValue();
        List<Map.Entry> sorted = titleCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toList());

        Hashtable<String, Long> dict = new Hashtable<String, Long>();

        int idDict = 1;
        for (Map.Entry<String, Long> entry : sorted) {
            System.out.println(idDict + "---" + entry.getKey() + " : " + entry.getValue());
            dict.put(entry.getKey(), entry.getValue());
            idDict += 1;
        }
        
        dict.forEach((key, value) -> {
            SparkService.setDict(key, value, sparkContext);
        });
    }

}
