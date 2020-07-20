package com.spark_basics;

import com.spark_basics.utils.Util;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/winutils");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> allSubtitles = sc.textFile("E:/Projects/BigData/Spark Java Projects/Spark basics and RDD/src/main/resources/subtitles/input.txt");

        JavaPairRDD<Long, String> countedWords = allSubtitles
                .map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
                .filter(sentence -> sentence.trim().length() > 0)
                .filter(word -> Util.isNotBoring(word))
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey((v1, v2) -> v1 + v2)
                .mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1))
                .sortByKey(false);

        countedWords.take(10).forEach(System.out::println);

        sc.close();
    }
}
