package com.spark_basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Basics {
    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> myRDD =  sc.parallelize(inputData);

        Integer result = myRDD.reduce((v1,v2) ->  v1 + v2);
        System.out.println(result);

        JavaRDD<Double> sqrtRDD = myRDD.map( value -> Math.sqrt(value));

        sqrtRDD.foreach( v -> System.out.println(v));
        //        sqrtRDD.collect().forEach(System.out::println);


        sc.close();
    }
}
