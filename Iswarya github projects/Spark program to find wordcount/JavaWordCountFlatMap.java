//LAB4B -Iswarya
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;

public class JavaWordCountFlatMap {
 
    public static void main(String[] args) {
        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Java Word Count FlatMap")
        .setMaster("spark://cssmpi1.uwb.edu:58200").set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // provide path to input text file
        String path = "sample.txt";
        
        // read text file to RDD
        JavaRDD<String> lines = sc.textFile(path);

        // Java with functions; split the input string into words
        JavaRDD<String> words = lines.flatMap( 
                                                new FlatMapFunction<String, String>(){
                                                
                                                    //public Iterable<String> call(String line) 
                                                    @Override
                                                    public Iterator<String> call (String s)                                                
                                                    {
                                                    return Arrays.asList(s.split(" ")).iterator();

                                                    }
                                            } 
                                            );
        
        // print #words
        System.out.println( "#words = " + words.count( ) );
    }
 
}
