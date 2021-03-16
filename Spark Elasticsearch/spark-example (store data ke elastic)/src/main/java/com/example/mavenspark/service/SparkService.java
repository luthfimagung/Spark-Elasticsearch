package com.example.mavenspark.service;

import org.sparkproject.guava.collect.ImmutableList;
import org.sparkproject.guava.collect.ImmutableMap;

import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

public class SparkService {
    
    public static String extractTitle(String videoLine) {
        try {
            return videoLine.split(",")[2];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }

    public static void setDict(String key, Long value, JavaSparkContext sparkContext){
            Map<String, ?> wordsStored = ImmutableMap.of("Title", key, "Counts", value);
            JavaRDD<Map<String, ?>> javaRDD = sparkContext.parallelize(ImmutableList.of(wordsStored));
            JavaEsSpark.saveToEs(javaRDD, "sparkelastic/docs");
            // id += 1;
            System.out.println(key + " : " + value);
    }

}
