package com.rishabh.nasaLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class logs {

    public static void main(String[] args) throws Exception {

        // spark configure
        SparkConf conf = new SparkConf().setName("hosts").setMaster("local[1]");
        // new java spark context
        JavaSparkContext sc = new JavaSparkContext(conf);
        // using Java RDD for july's logs
        JavaRDD<String> julyLogs = sc.textFile("src/main/resources/contents/log1.tsv");
        // Java RDD for august's logs
        JavaRDD<String> augustLogs = sc.textFile("src/main/resources/contents/log2.tsv");
        // july hosts mapping
        JavaRDD<String> julyHosts = julyLogs.map(line -> line.split("\t")[0]);
        // augusts hosts mapping
        JavaRDD<String> augustHosts = augustLogs.map(line -> line.split("\t")[0]);

        JavaRDD<String> intersect = julyHosts.intersect(augustHosts);
        JavaRDD<String> cleanIntersection = intersect.filter(host -> !host.equals("host"));

        cleanIntersection.saveAsTextFile("src/main/resources/contents/nasaLogs.txt")

    }
}
