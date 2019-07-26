package com.iot;

import org.apache.hadoop.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.*;

public class Main {

    public static void main(String[] args) {
//        Конфигурирование из нета.
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("JavaLogQuery")
//                .config("spark.master", "local")
//                .getOrCreate();
        System.setProperty("hadoop.home.dir", "/");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        //Загрузка из файла
        JavaRDD<String> textFile = sc.textFile("src/main/resources/README.md");
        System.out.printf(String.valueOf(textFile.count()));
        System.out.println(textFile.first());


        //Отфильтровывание всех строк, содержащих Python
        JavaRDD<String> filterLines = textFile.filter(line -> line.contains("Python"));
        for (String s : filterLines.collect()) {
            System.out.println(s);
        }

        //Разбивка файла на слова
        JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return (Iterator<String>) asList(s.split(" "));
            }
        });


//        System.out.println("$$$$$$$");
//        JavaPairRDD<String, Integer> countWords = words.mapToPair(e -> e.split(" ")).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer x, Integer y) throws Exception {
//                return x + y;
//            }
//        });
//        System.out.println(countWords.count());
//        System.out.println("$$$$$$$");

        JavaRDD<String> colect = sc.parallelize(asList("one", "two", "three"));

        JavaRDD<String> inputRDD = sc.textFile("src/main/resources/syslog");
        JavaRDD<String> errorsRDD = inputRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("kernel");
            }
        });
//        Тоже самое, но с лямбда выражением
        JavaRDD<String> errorsRDD2 = inputRDD.filter(s -> s.contains("kernel"));
        System.out.println("---------");
        System.out.println(StringUtils.join(",", errorsRDD2.collect()));

        for (String s : errorsRDD.take(10)) { //Выводит первые 10 записей множества
            System.out.println(s);
        }

        System.out.println("Count strings with contains kernel: " + errorsRDD.count());

        for (String s : errorsRDD.countByValue().keySet()) {
            System.out.println(s + "    " + errorsRDD.countByValue().get(s));
        }


        PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[5], s);
            }
        };
        JavaPairRDD<String, String> pairs = errorsRDD2.mapToPair(keyData);
        for (String key : pairs.collectAsMap().keySet()) {
            System.out.println(key + pairs.collectAsMap().get(key));
        }

        SparkSession s = SparkSession.builder().config("spark.master", "local").getOrCreate();
        TestModel[] arr = new TestModel[2];

        arr[0] = new TestModel();
        arr[0].setId(1);
        arr[0].setName("One");

        arr[1] = new TestModel();
        arr[0].setId(2);
        arr[0].setName("Two");

        List<TestModel> list = new ArrayList<>();
        list.add(arr[0]);
        list.add(arr[1]);

        Dataset<TestModel> ds = s.createDataset(list, Encoders.bean(TestModel.class));
        System.out.println("!!!!!!!!!!" + ds.count());

    }
}
