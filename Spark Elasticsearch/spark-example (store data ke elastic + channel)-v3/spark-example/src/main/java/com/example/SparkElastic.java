package com.example;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

// import scala.Serializable;

public class SparkElastic implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper mapper = new ObjectMapper();
    
    public static void main(String[] args){

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("SparkCSVElastic").setMaster("local[2]")
                .set("es.index.auto.create", "true")
                .set("es.nodes.wan.only", "true")
                .set("es.nodes", "192.168.20.245:9200");
                // .set("es.batch.size.bytes", "300000000")
                // .set("es.http.timeout", "5s")
                // .set("es.http.retries", "1");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // JavaRDD<String> dataRDD = sparkContext.textFile("C:/Users/Luthfi M Agung/archive/csv2/KRvideos2.csv");
        // JavaRDD<String> dataRDD = sparkContext.textFile("C:/Users/Luthfi M Agung/archive/csv/JPvideos.csv");
        JavaRDD<String> dataRDD = sparkContext.textFile("C:/Users/Luthfi M Agung/archive/csv/*");
        JavaRDD<String> dataRDDMap = dataRDD.map(new Function<String, String>(){
            private static final long serialVersionUID = 1L;

            // @Override
            public String call(String value) throws Exception {
                String json = "";
                try {
                    Map<String, Object> result = new HashMap<String, Object>();
                    // String[] datas = value.split(";");
                    String[] datas = value.split(",");
                    int index = 3;
                    
                    try {
                        if (!datas[0].startsWith("\\n") && (datas[1] != null && datas[1].length() == 8)){ 
                            result.put("video_id", datas[0]);
                            result.put("trending_date", datas[1]);

                            if (datas[2].startsWith("\"") && !datas[2].endsWith("\"")) {
                                while (!datas[index].endsWith("\"")) {
                                    index += 1;
                                }
                                try {
                                    result.put("title", datas[2] + " " + datas[index]);
                                    
                                    int channelIndex = index+1;
                                    if (datas[channelIndex].startsWith("\"") && !datas[channelIndex].endsWith("\"")) {
                                        while (!datas[channelIndex].endsWith("\"")) {
                                            channelIndex += 1;
                                        }
                                        result.put("channel_title", datas[index + 1] + " " + datas[channelIndex]);
                                        result.put("category_id", datas[channelIndex + 1]);
                                        result.put("publish_time", datas[channelIndex + 2]+".");
                                        result.put("tags", datas[channelIndex + 3]);
                                        result.put("views", datas[channelIndex + 4]);
                                        result.put("likes", datas[channelIndex + 5]);
                                        result.put("dislikes", datas[channelIndex + 6]);
                                        result.put("comment_count", datas[channelIndex + 7]);
                                        result.put("thumbnail_link", datas[channelIndex + 8]);
                                        result.put("comments_disabled", datas[channelIndex + 9]);
                                        result.put("ratings_disabled", datas[channelIndex + 10]);
                                        result.put("video_error_or_removed", datas[channelIndex + 11]);
                                        result.put("description", datas[channelIndex + 12]);
                                    }else {
                                        result.put("channel_title", datas[index + 1]);
                                        result.put("category_id", datas[index + 2]);
                                        result.put("publish_time", datas[index + 3] + ".");
                                        result.put("tags", datas[index + 4]);
                                        result.put("views", datas[index + 5]);
                                        result.put("likes", datas[index + 6]);
                                        result.put("dislikes", datas[index + 7]);
                                        result.put("comment_count", datas[index + 8]);
                                        result.put("thumbnail_link", datas[index + 9]);
                                        result.put("comments_disabled", datas[index + 10]);
                                        result.put("ratings_disabled", datas[index + 11]);
                                        result.put("video_error_or_removed", datas[index + 12]);
                                        result.put("description", datas[index + 13]);
                                    }
                                } catch (Exception e) {

                                }
                            } else {
                                try {
                                    result.put("title", datas[2]);

                                    int channelIndex = 3;
                                    if (datas[channelIndex].startsWith("\"") && !datas[channelIndex].endsWith("\"")) {
                                        while (!datas[channelIndex].endsWith("\"")) {
                                            channelIndex += 1;
                                        }
                                        result.put("channel_title", datas[3] + " " + datas[channelIndex]);
                                        result.put("category_id", datas[channelIndex + 1]);
                                        result.put("publish_time", datas[channelIndex + 2] + ".");
                                        result.put("tags", datas[channelIndex + 3]);
                                        result.put("views", datas[channelIndex + 4]);
                                        result.put("likes", datas[channelIndex + 5]);
                                        result.put("dislikes", datas[channelIndex + 6]);
                                        result.put("comment_count", datas[channelIndex + 7]);
                                        result.put("thumbnail_link", datas[channelIndex + 8]);
                                        result.put("comments_disabled", datas[channelIndex + 9]);
                                        result.put("ratings_disabled", datas[channelIndex + 10]);
                                        result.put("video_error_or_removed", datas[channelIndex + 11]);
                                        result.put("description", datas[channelIndex + 12]);
                                    }else {
                                        result.put("channel_title", datas[3]);
                                        result.put("category_id", datas[4]);
                                        result.put("publish_time", datas[5] + ".");
                                        result.put("tags", datas[6]);
                                        result.put("views", datas[7]);
                                        result.put("likes", datas[8]);
                                        result.put("dislikes", datas[9]);
                                        result.put("comment_count", datas[10]);
                                        result.put("thumbnail_link", datas[11]);
                                        result.put("comments_disabled", datas[12]);
                                        result.put("ratings_disabled", datas[13]);
                                        result.put("video_error_or_removed", datas[14]);
                                        result.put("description", datas[15]);
                                    }
                                } catch (Exception e) {

                                }
                            }
                        }else{
                            // result.put("video_id", null);
                        }
                    } catch (Exception e) {
                        
                    }
                    
                    json = mapper.writeValueAsString(result);

                } catch (Exception e) {
                    e.getMessage();
                }
                return json;
			}
            
        });

        JavaRDD<String> dataRDDMapFilter = dataRDDMap.filter(new Function<String, Boolean>(){
            private static final long serialVersionUID = 1L;
            final String header = dataRDDMap.first();
            Boolean ignore;
            
            // @Override
            public Boolean call(String s) throws Exception {
                try {
                    ignore = !s.equalsIgnoreCase(header);
                } catch (Exception e) {
                    
                }
                return ignore;
            }

        });
        
        // dataRDD.foreach(x -> 
        //     System.out.println(x)
        // );
                
        try {
            System.out.println("Saving " + dataRDDMapFilter.count() + " data. Please Wait.");
            JavaEsSpark.saveJsonToEs(dataRDDMapFilter, "sparkelastic-v6/docs");
            System.out.println("Finish.");
        } catch (Exception e) {
            e.getMessage();
        }

        sparkContext.close();
    }

}