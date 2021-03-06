import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;


public class wordCount {

    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("wordCount");
        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String> input=sc.textFile("./input.txt");

        JavaRDD<String> words=input.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String x){
                        return Arrays.asList((T) x.split(" "));
                    }
                }
        );

        JavaPairRDD<String ,Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String x) throws Exception {
                        return new Tuple2<>(x,1);
                    }
                }
        ).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer x, Integer y) throws Exception {
                return x+y;
            }
        });

        counts.saveAsTextFile("./output.txt");
    }
}
