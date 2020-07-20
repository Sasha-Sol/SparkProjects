package com.spark_basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatMap {
    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 1999");
        inputData.add("ERROR: Friday 6 September 2012");
        inputData.add("FATAL: Wednesday 10 September 2005");
        inputData.add("ERROR: Friday 15 September 2002");
        inputData.add("WARN: January 23 September 2007");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.parallelize(inputData)
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .filter(str -> str.length() > 1)
                .collect()
                .forEach(System.out::println);

        sc.close();
    }
}
