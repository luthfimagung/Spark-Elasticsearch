package com.example.mavenspark.service;

import org.sparkproject.guava.collect.ImmutableList;
import org.sparkproject.guava.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

public class SparkService {
    
    public static String extractIdfromCSV(String videoLine) {
        try {
            // if (!videoLine.split(";")[11].isEmpty()){
            return videoLine.split(";")[0];
            // }else
                // return null;
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }

    public static String extractTitlefromCSV(String videoLine) {
        try {
            // if (videoLine.split(";")[3].isEmpty() || videoLine.split(";")[3].isBlank()) {
            //     if (videoLine.split(";")[4].isEmpty() || videoLine.split(";")[4].isBlank()){
            //         if(videoLine.split(";")[5].isEmpty() || videoLine.split(";")[5].isBlank()){
            //             if(videoLine.split(";")[6].isEmpty() || videoLine.split(";")[6].isBlank()){
            //                 return videoLine.split(";")[2];
            //             }else
            //                 return videoLine.split(";")[2] + " ;;;; " + videoLine.split(";")[6];
            //         }else
            //             return videoLine.split(";")[2] + " ;;; " + videoLine.split(";")[5];
            //             // }else
            //             //     return videoLine.split(";")[2] ;
            //     }else if(videoLine.split(";")[4].contains("\"")){
            //             return videoLine.split(";")[2] + " ;; " + videoLine.split(";")[4];
            //         }else
            //             return videoLine.split(";")[2];
            // }else if (videoLine.split(";")[3].contains("\"")){
            //         return videoLine.split(";")[2] + " ; " + videoLine.split(";")[3];
            //     }else
            //         return videoLine.split(";")[2];
            int index = 2;
            if(videoLine.split(";")[2].startsWith("\"") && videoLine.split(";")[2].contains("\"")){
                while(!videoLine.split(";")[index].endsWith("\"")){
                    index += 1;
                }
                return videoLine.split(";")[2] + " " + videoLine.split(";")[index];
            }else
                return videoLine.split(";")[2];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }

    public static String extractChannelfromCSV(String videoLine) {
        try {
            // if (!videoLine.split(";")[3].isEmpty() || !videoLine.split(";")[3].isBlank()){
            //     return videoLine.split(";")[3];
            // }else
            //     return "error : delimiter (;) found on title.";
            int index = 2;
            if (videoLine.split(";")[2].startsWith("\"") && videoLine.split(";")[2].contains("\"")) {
                while (!videoLine.split(";")[index].endsWith("\"")) {
                    index += 1;
                }
                return videoLine.split(";")[index+1];
            } else
                return videoLine.split(";")[3];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }

    public static JavaRDD<String> extractVideoId(JavaRDD<String> videos) {
        JavaRDD<String> video_id = videos.map(SparkService::extractIdfromCSV).filter(StringUtils::isNotBlank);
        return video_id;
    }

    public static JavaRDD<String> extractTitle(JavaRDD<String> videos){
        JavaRDD<String> titles = videos.map(SparkService::extractTitlefromCSV).filter(StringUtils::isNotBlank);
        // Map<String, Long> titleCounts = eachTitle.countByValue();
        // List<Map.Entry> sorted = titleCounts.entrySet().stream().sorted(Map.Entry.comparingByValue())
        //         .collect(Collectors.toList());
        return titles;
    }

    public static JavaRDD<String> extractChannel(JavaRDD<String> videos){
        JavaRDD<String> channels = videos.map(SparkService::extractChannelfromCSV).filter(StringUtils::isNotBlank);
        return channels;
    }

    public static Iterator<String> extractIterator(JavaRDD<String> method){
        JavaRDD<String> object = method
                .flatMap(x -> Arrays.asList(x.trim()).iterator());

        return object.collect().iterator();
    }

    public static void exportVideoInfo(Object id, Object title, Object channel, JavaSparkContext sparkContext, int count){
            Map<String, ?> wordsStored = ImmutableMap.of("VideoId", id, "Title", title, "Channel", channel);
            JavaRDD<Map<String, ?>> javaRDD = sparkContext.parallelize(ImmutableList.of(wordsStored));
            JavaEsSpark.saveToEs(javaRDD, "sparkelastic-v3/docs");
            // count += 1;
            System.out.println(count + " | " + id + " | " + title + " : " + channel);
    }

}
