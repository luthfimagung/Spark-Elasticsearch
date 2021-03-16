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
    private static String video_id = "";
    private static String description = "";
    private static String newLineDesc = "";
    public static void main(String[] args){

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("SparkCSVElastic").setMaster("local[2]")
                .set("es.index.auto.create", "true")
                .set("es.nodes.wan.only", "true")
                .set("es.nodes", "192.168.20.245:9200")
                .set("es.batch.size.bytes", "300000000")
                .set("es.http.timeout", "5s")
                .set("es.http.retries", "1");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> dataRDD = sparkContext.textFile("C:/Users/Luthfi M Agung/archive/KRvideos2.csv");
        JavaRDD<String> dataRDDMap = dataRDD.map(new Function<String, String>(){
            private static final long serialVersionUID = 1L;

            // @Override
            public String call(String value) throws Exception {
                String json = "";
                try {
                    Map<String, Object> result = new HashMap<String, Object>();
                    String[] datas = value.split(";");
                    int index = 3;
                    
                    try {
                        if (datas[1] != null && datas[1].length() == 8){
                            video_id = datas[0];
                            if (datas[2].startsWith("\"")) {
                                while (!datas[index].endsWith("\"")) {
                                    index += 1;
                                }
                                if (datas[index + 13].startsWith("\"")){
                                    description = datas[index + 13];
                                }else{
                                    result.put("video_id", video_id);
                                    result.put("description", datas[index + 13]);
                                }
                            }else{
                                if (datas[15].startsWith("\"")) {
                                    description = datas[15];
                                } else {
                                    result.put("video_id", video_id);
                                    result.put("description", datas[15]);
                                }
                            }
                        }else{
                            if (datas[0].endsWith("\"")){
                                newLineDesc = datas[0];
                                description = description + newLineDesc;
                                result.put("video_id", video_id);
                                result.put("description", description);
                            }else{
                                newLineDesc = datas[0];
                                description = description + newLineDesc;
                            }
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
            System.out.println("Saving...");
            JavaEsSpark.saveJsonToEs(dataRDDMapFilter, "sparkelastic-v6/docs");
            System.out.println("Finish.");
        } catch (Exception e) {
            e.getMessage();
        }

        sparkContext.close();
    }

}
