/**
 * CSS534 - Prog 4: Parallelizing shortest path search
 * using BFS approach in Spark
 * Author: Created by Iswarya on 11/17/2019.
 */

import java.io.Serializable;
import java.util.*;
import java.lang.*;
import scala.Tuple2;
import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;                 // Spark Configuration
import org.apache.spark.api.java.JavaSparkContext; // Spark Context created from SparkConf
import java.util.Arrays;
import java.util.Map;
import java.util.Comparator;
import java.io.*;
import org.apache.spark.api.java.function.VoidFunction;

public class ShortestPath {

    public static void main(String args[]) throws Exception {
        //
        String inputFile = args[0];         //Input file containing graph data
        String source_vertex_str = args[1]; //Source vertex used to find shortest distance
        String dest_vertex_str = args[2];   //Destination vertex used to find shortest distance

        //Status of vertices
        final String active_str = "ACTIVE";
        final String inactive_str = "INACTIVE";

        // start Sparks and read a given input file
        SparkConf conf = new SparkConf().setAppName("BFS-based Shortest Path Search");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile(inputFile);

        // starting a timer
        long startTime = System.currentTimeMillis();

        //Map contents of input file to JavapairRDD<String, Data>
        JavaPairRDD<String, Data> network = lines.mapToPair(line -> {

            //Split each line in file and create Data objects and tuples
            String[] split_line = line.split("=");
            String key_vertex = split_line[0];
            String status = String.valueOf(inactive_str);
            Integer distance_initial = Integer.MAX_VALUE;
            Integer prev_dist = Integer.MAX_VALUE;

            //If source vertex, then set its inital parameters - ACTIVE status and distance = 0
            if (key_vertex.equals(source_vertex_str)) {
                status = String.valueOf(active_str);
                distance_initial = 0;
                prev_dist = 0;
            }

            //If neighbours exists for a vertex, then create tuples of neighbors in data, by updating its distance
            if (split_line.length > 1) {
                String[] vertex_attr = split_line[1].split(";");
                List<Tuple2<String, Integer>> neighbours = new ArrayList<>();
                for (String s : vertex_attr) {
                    String[] neighbours_attr = s.split(",");
                    Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(neighbours_attr[0], Integer.parseInt(neighbours_attr[1]));
                    neighbours.add(tuple);
                }
                Data vertex_data = new Data(neighbours, distance_initial, prev_dist, status);
                return new Tuple2<String, Data>(key_vertex, vertex_data);
            }
            //If no neighbours for a vertex, then return with empty data object
            else {
                Data vertex_data = new Data();
                return new Tuple2<String, Data>(key_vertex, vertex_data);
            }
        });

        //Store the network tuples in Map for fast access
        Map<String, Data> network_map = network.collectAsMap();
        //Keep track of number of active vertices in the network RDD
        long active_vertex_count = network.filter(data_ele -> (data_ele._2().status.equals(active_str))).count();

        //Execute while loop until there are active vertices in the RDD
        while ((int) active_vertex_count > 0) {

            // If status='active', then create tuples for their neighbours and add to a list and return it
            JavaPairRDD<String, Data> propagatedNetwork = network.flatMapToPair(vertex -> {
                //List to store all vertices - to be returned
                List<Tuple2<String, Data>> all_vertices = new ArrayList<>();
                // for the current active vertices: add the neighbours data
                if (vertex._2().status.equals(active_str)) {
                    Iterator<Tuple2<String, Integer>> neighbour_list = vertex._2().neighbors.iterator();
                    while (neighbour_list.hasNext()) {
                        Tuple2<String, Integer> t = neighbour_list.next();
                        //Retreive neighbor data from original network- using map.get()
                        Data d = network_map.get(t._1());
                        Integer updated_distance = t._2() + vertex._2().distance;
                        Tuple2<String, Data> tuple = new Tuple2<String, Data>(t._1(), new Data(d.neighbors, updated_distance, d.prev, d.status));
                        all_vertices.add(tuple);
                    }
                }
                //Add all vertices to propogated netwwork RDD
                all_vertices.add(new Tuple2<>(vertex._1(), vertex._2()));
                return all_vertices.iterator();
            });

            // For each key, (i.e., each vertex), find the shortest distance and
            // return the corresponding value(Data object)
            network = propagatedNetwork.reduceByKey((k1, k2) -> {
                Integer k1_d = k1.distance;
                Integer k2_d = k2.distance;
                return k1_d < k2_d ? k1 : k2;
            });

            // If a vertex’s new distance is shorter than prev, activate this vertex
            // status and replace prev with the new distance.
            network = network.mapValues(value -> {
                if (value.distance < value.prev) {
                    value.prev = value.distance;
                    value.status = String.valueOf(active_str);
                } else {
                    value.status = String.valueOf(inactive_str);
                }

                return value;
            });
            //update the current number of active vertices
            active_vertex_count = network.filter(data_ele -> (data_ele._2().status.equals(active_str))).count(); 
        }

        //Elapsed time of execution of algorithm to find the shortest distance.
        long estimatedTime = System.currentTimeMillis() - startTime;

        //Print the distance from source vertex to destination vertex.
        List<Tuple2<String, Data>> resultlist = network.collect();
        for (Tuple2<String, Data> vertx : resultlist) {
            if (vertx._1().equals(dest_vertex_str))
                System.err.println("from " + source_vertex_str + " to " + vertx._1() + " takes  distance = " + vertx._2().distance);
        }        
        System.err.println("Elapsed time(in milliseconds): " + estimatedTime);

        //Stop the spark context
        jsc.stop();
    }
}

