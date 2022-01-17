/**
 * ACO Based parallelization of TSP with Map-Reduce program in hadoop
 * Created by Iswarya Hariharan on 12/3/2019.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Random;

/**
 * ACOTsp class implements the mapper, reducer functions for the Parallel TSP
 * with Ant colonial optimisation based algorithm
 */

public class ACOTsp {

    //Results of tour distance and path
    public static Double result_optimal_distance;
    public static String result_optimal_path;

    /**
     * Mapper class for ACO based TSP.
     * The base class Mapper is parameterized by
     * <in key type, in value type, out key type, out value type>.
     * Thus, this mapper takes (LongWritable doc_Id, Text value) pairs and outputs
     * (Text key, Text value) pairs. The input keys are assumed
     * to be identifiers for documents, which are ignored, and the values
     * to be the content of documents which is also ignored. The documents are created just to
     * spawn the mapper tasks as number of mapper tasks=number of input splits.
     * The parameters common for the mappers and program are accessed using
     * the JobConf object. The output key is "Result" (To ensure only one reduce task is created) and the
     * and the output values are of the format: "path;allcities visited?;distancecovered" for each ant.
     */
    public static class MapTsp extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        //Common variables for Mapper class
        private Text doc_count = new Text();
        private String filecontent;
        double pheromone_matrix[][];
        double adjacency_matrix[][];
        double alpha, beta, evaporation, Q;

        //Job conf object to get the arguments
        JobConf conf;

        /**
         * @param JobConf job configuration object to get
         *                job configuration information
         */
        public void configure(JobConf job) {
            this.conf = job;
        }

        /**
         * Actual map function. Takes one document's text and emits key-value
         * pairs for every ant that is generated from a random vertex with
         * its path travelled and distance of the path.
         *
         * @param docId           Document identifier (ignored).
         * @param value           of the current document(ignored).
         * @param OutputCollector object for accessing output,
         *                        configuration information, etc.
         * @reporter For getting the filename and path
         */

        public void map(LongWritable doc_Id, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            //Number of vertices in the graph- obtained from configuration object
            int vertices_num = Integer.parseInt(conf.get("verticesnum"));
            //Adjacency matrix
            this.pheromone_matrix = new double[vertices_num + 1][vertices_num + 1];
            //Pheromone matrix
            this.adjacency_matrix = new double[vertices_num + 1][vertices_num + 1];

            //Get the common constants from configuration and assign default values
            alpha = Double.parseDouble(conf.get("alpha", "1.0"));
            evaporation = Double.parseDouble(conf.get("evaporation", "0.5"));
            beta = Double.parseDouble(conf.get("beta", "2.0"));
            Q = Double.parseDouble(conf.get("Q", "4"));

            //Pheromone matrix filename
            String filepath = conf.get("pheromonefilename");
            String pheromone_matrix_str = convertArrayToString(vertices_num, pheromone_matrix);

            //Create adjacency matrix and pheromone matrix from the corresponding data passed from conf object
            String graph_data = conf.get("graph");
            String pheromone_matrix_data = conf.get("pheromonedata");
            String vertices_graph_data[] = graph_data.split("\n");
            String pheromone_graph_data[] = pheromone_matrix_data.split("\n");
            for (int i = 0; i <= vertices_num; i++) {
                {
                    String edge_weight[] = vertices_graph_data[i].split(",");
                    String pheromone_edge_weight[] = pheromone_graph_data[i].split(",");
                    for (int j = 0; j <= vertices_num; j++) {
                        adjacency_matrix[i][j] = Double.parseDouble(edge_weight[j].trim());
                        pheromone_matrix[i][j] = Double.parseDouble(pheromone_edge_weight[j].trim());
                    }
                }
            }

            //for all the Ants output string computation
            StringBuilder ants_output = new StringBuilder();
            //Prefix multiple ants data with ~
            String prefix = "";
            Random random = new Random();
            for (int i = 1; i < 3; i++) {
                //generate the random node for an ant's start position
                int rand_int = random.nextInt(vertices_num + 1);
                if (rand_int != 0) {
                    ants_output.append(prefix);
                    prefix = "~";
                    ants_output.append(computeAntPath(rand_int, vertices_num));

                } else
                    i--;    //Repeat the loop if the vertex-id selected is 0, as nvertices id start with '1'
            }
            doc_count.set(ants_output.toString());
            //Create key as a constant value so that number of reduce tasks spawned=1
            output.collect(new Text("Result"), doc_count);
        }

        /*
        Compute the path taken by the ant using the probabality
        adjacency matrix and pheromone matrix are used used here for this computation.
         */
        private String computeAntPath(int start_position, int vertices_num) {

            boolean visited[] = new boolean[vertices_num + 1];      //visited array to store the cities the ant has visited-by default all values initialised to false
            StringBuilder path_taken = new StringBuilder();         //to compute the output data of the ant
            Double distance_covered = 0.0;                          //distance covered by the ant in the tour
            int current_node = start_position;                      //vertex the ant is currently
            int chosen_vertex = start_position;                     //chosen vertex for the next movement
            visited[current_node] = visited[0] = true;
            path_taken.append(String.valueOf(current_node));
            Double most_probable_value = Double.MIN_VALUE;          //highest probable edge to take
            double vertex_probablity;
            while (!check_allvertices_visited(visited)) {           //When at least one vertex is not yet visited
                most_probable_value = Double.MIN_VALUE;
                chosen_vertex = current_node;                       //vertex to check feasibilty of movement
                for (int i = 1; i <= vertices_num; i++) {           //from the chosen vertex, calculate the probability of each edge to take
                    if (current_node != i && !visited[i] && adjacency_matrix[current_node][i] > 0.0) {
                        vertex_probablity = Math.pow(pheromone_matrix[current_node][i], alpha) * Math.pow((double) ((1.0) / adjacency_matrix[current_node][i]), beta);
                        if (vertex_probablity > most_probable_value) {  //Update the most probable edge to take
                            most_probable_value = vertex_probablity;
                            chosen_vertex = i;
                        }
                    }
                }
                if (chosen_vertex == current_node) {                //Exit the loop- as ant cant move to any other vertex as there is no path existing
                    break;
                } else {                                            //update the ant's path by moving to the chosen vertex and distance of the path
                    distance_covered += adjacency_matrix[current_node][chosen_vertex];
                    current_node = chosen_vertex;
                    visited[current_node] = true;
                    path_taken.append("," + String.valueOf(current_node));
                }
            }
            boolean full_path = check_allvertices_visited(visited);  //Output of ant data in the form: "path;allcities visited?;distancecovered"
            path_taken.append(";");
            path_taken.append(full_path);
            path_taken.append(";");
            path_taken.append(String.valueOf(distance_covered));
            return path_taken.toString();
        }

        /*Check if all vertices are visited -
        return true: if all vertices are visited, false: one vertex also not visited
         */
        private boolean check_allvertices_visited(boolean visited[]) {
            for (boolean k : visited) {
                if (!k)
                    return false;
            }
            return true;
        }
    }


    /**
     * Reducer class for ACO based parallel TSP.
     * <p>
     * Like the Mapper base class, the base class Reducer is parameterized by
     * <in key type, in value type, out key type, out value type>.
     * <p>
     * For each Text key, which represents a word(There is only one reducer here: with key="Result",
     * this reducer gets a list of
     * Text values, computes the optimal path and distance - which is the result
     * The pheromone matrix is converted to a string and sent along with output
     * to pass on to the next iteration from driver class
     */
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        //Common variables for Mapper class
        double pheromone_matrix[][];
        double adjacency_matrix[][];
        double alpha, beta, evaporation, Q;

        //Job conf object to get the arguments
        JobConf conf;

        /**
         * @param JobConf job configuration object to get
         *                job configuration information
         */
        public void configure(JobConf job) {
            this.conf = job;
        }

        /**
         * Actual reduce function.
         *
         * @param key             Key - 'Result' is the only key, hence only one reducer is spawned.
         * @param values          Iterator over the text for this key: 'result'. Iterator over all the ants data.
         * @param OutputCollector object for accessing output and putting values,
         *                        configuration information, etc.
         * @param Reporter        ignored
         */
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            Text doc_list = new Text();
            String doc_name, value_string;
            String optimal_path = "";
            StringBuilder doc_list_str = new StringBuilder();

            //Number of vertices in the graph- obtained from configuration object
            int vertices_num = Integer.parseInt(conf.get("verticesnum"));
            //Pheromone matrix
            this.pheromone_matrix = new double[vertices_num + 1][vertices_num + 1];
            //Adjacency matrix
            this.adjacency_matrix = new double[vertices_num + 1][vertices_num + 1];

            //Get the constant parameters from configuration object
            alpha = Double.parseDouble(conf.get("alpha", "1.0"));
            evaporation = Double.parseDouble(conf.get("evaporation", "0.5"));
            beta = Double.parseDouble(conf.get("beta", "2.0"));
            Q = Double.parseDouble(conf.get("Q", "4"));

            //Get the previous iteration's optimal distance and optimal path of the ants
            Double optimal_distance = Double.parseDouble(conf.get("optimaldistance", String.valueOf(Double.MAX_VALUE)));
            optimal_path = (conf.get("optimalpath", ""));

            //Read the pheromone matrix and evaporate it
            String pheromone_matrix_data = conf.get("pheromonedata");
            String pheromone_graph_data[] = pheromone_matrix_data.split("\n");
            for (int i = 0; i <= vertices_num; i++) {
                {
                    String pheromone_edge_weight[] = pheromone_graph_data[i].split(",");
                    for (int j = 0; j <= vertices_num; j++) {
                        //Evaporate the pheromone value for every edge
                        pheromone_matrix[i][j] = (1 - evaporation) * Double.parseDouble(pheromone_edge_weight[j].trim());
                    }
                }
            }

            Double ant_delta_val = 0.0;
            while (values.hasNext()) {                      //For all the ants data received in values - iterate over it
                Text value = values.next();
                value_string = value.toString();
                String ants_data[] = value_string.split("~");
                for (String ant_data : ants_data) {
                    String ants_data_seperated[] = ant_data.split(";");
                    Double ant_dist = Double.parseDouble(ants_data_seperated[2]);
                    if (ant_dist > 0.0)
                        ant_delta_val = Q / ant_dist;       //Compute the value of each ant's delta for pheromone to be updated
                    else
                        ant_delta_val = 0.0;
                    String[] ant_tour_vertices = ants_data_seperated[0].split(",");
                    int ant_tour_length = ant_tour_vertices.length;
                    for (int i = 0; i < ant_tour_length - 1; i++) {
                        int index1 = Integer.parseInt(ant_tour_vertices[i]);
                        int index2 = Integer.parseInt(ant_tour_vertices[i + 1]);
                        pheromone_matrix[index1][index2] += ant_delta_val;      //Update the pheromone matrix with the change in pheromone-ant_delta
                        pheromone_matrix[index2][index1] = pheromone_matrix[index1][index2];
                    }
                    if (ants_data_seperated[1].equals("true") && ant_dist < optimal_distance) { //Update the optimal distance obtained with least distance of an ant's tour
                        optimal_path = ants_data_seperated[0];
                        optimal_distance = ant_dist;
                    }
                }
            }

            String outputdata;
            doc_list_str.append("Optimal distance:" + optimal_distance + "; Optimal path:" + optimal_path);
            //append new line to output text for starting pheromone matrix data in the next line
            doc_list_str.append("\n");
            //append pheromone matrix to the output text
            doc_list_str.append(convertArrayToString(vertices_num, pheromone_matrix));
            outputdata = doc_list_str.toString();
            doc_list.set(outputdata);
            Text word = new Text("Result");
            output.collect(word, doc_list);
        }
    }


    /**
     * Combiner for ACOTsp parallelization.
     * <p>
     * Like the Mapper base class, the base class Combine is parameterized by
     * <in key type, in value type, out key type, out value type>.
     * <p>
     * For each Text key, which represents a word, this combiner gets a list of
     * Text values, sends those values to the reducer
     * It ensures all the values from the mapper are sent to only one reducer
     */
    public static class Combine extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        //Job conf object to get the arguments
        JobConf conf;

        /**
         * @param JobConf job configuration object to get
         *                job configuration information
         */
        public void configure(JobConf job) {
            this.conf = job;
        }

        /**
         * Actual combine function.
         *
         * @param key             Key - 'Result' is the only key, hence only one reducer is spawned.
         * @param values          Iterator over the text for this key: 'result'. Iterator over all the ants data.
         * @param OutputCollector object for accessing output and putting values,
         *                        configuration information, etc.
         * @param Reporter        ignored
         */
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            //Send all values received from mapper to a single reducer
            StringBuilder resultstr = new StringBuilder();
            while (values.hasNext()) {
                resultstr.append(values.next().toString());
            }
            output.collect(key, new Text(resultstr.toString()));
        }

    }

    /*
     *  function to read file contents into a single string with line terminators
     *  and create the adjacency matrix and initialise it.
     */
    private static String readAllFileContentToString(String filepath, int vertices_num) {
        String content = "";
        try {
            content = new String(Files.readAllBytes(Paths.get(filepath)));  //File contains the adjacency list of the graph
        } catch (IOException e) {
            e.printStackTrace();
        }
        double adj_matrix[][] = new double[vertices_num + 1][vertices_num + 1];
        double min_dist = Double.MAX_VALUE, max_dist = Double.MIN_VALUE;
        String graph_lines[] = content.split("\\r?\\n");            //String manipulate and compute the adjacency matrix from the adjacency list of the graph
        for (String str : graph_lines) {
            int row_index = Integer.parseInt(str.split("=")[0]);
            String adj_list[] = str.split("=")[1].split(",");
            int col_index;
            double edge_dist;
            for (String node_data : adj_list) {
                col_index = Integer.parseInt(node_data.split(":")[0]);
                edge_dist = Integer.parseInt(node_data.split(":")[1]);
                adj_matrix[row_index][col_index] = edge_dist;
                //Track down the min and max distance of edges in the graph
                if (edge_dist > max_dist)
                    max_dist = edge_dist;
                if (edge_dist < min_dist && edge_dist > 0)
                    min_dist = edge_dist;
            }
        }
        for (int i = 0; i <= vertices_num; i++) {                       //For vertices that dont have an edge, add a value to indicate the edge weight
            for (int j = 0; j <= vertices_num; j++) {
                if (i != j && i > 0 && j > 0 && adj_matrix[i][j] == 0.0) {
                    min_dist += 10.0;
                    adj_matrix[i][j] = adj_matrix[j][i] = min_dist;
                }
            }
        }
        content = convertArrayToString(vertices_num, adj_matrix);
        return content;
    }

    /*
     * Function to read the fileoutput data of previous reduce and
     * create the pheromone matrix for the driver class to pass to
     * map and reduce functions in the next iteration
     * @param filepath: file name with Pheromone data
     * @param vertices_num : number of vertices in graph
     * @param onlypath_data: true if only optimal path to be taken from the file, false if pheromone data also to be updated from the file
     */
    private static String createPheromoneData(String filepath, int vertices_num, boolean only_pathdata) throws IOException {
        //pheromone matrix
        double pheromone_matrix[][] = new double[vertices_num + 1][vertices_num + 1];
        //Initialise pheromone matrix for the calculations in first iteration
        if (!(filepath.contains("part-00000"))) {
            for (int i = 0; i <= vertices_num; i++) {
                for (int j = 0; j <= vertices_num; j++) {
                    pheromone_matrix[i][j] = 1.0;
                }
            }
            result_optimal_distance = Double.MAX_VALUE;
            result_optimal_path = "";
        } else {
            Path pt = new Path(filepath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line = br.readLine();
            if (line != null) {             //Store the previous iteration's optimal path and distance
                String seperated_result[] = line.split(";");
                result_optimal_distance = Double.parseDouble(seperated_result[0].split(":")[1]);
                result_optimal_path = seperated_result[1].split(":")[1];
            }
            //Start parsing pheromone matrix from second line,
            // as first line contains the key, optimal path - output from the reducer
            if (!only_pathdata)             //Do only if pheromone data retrieval is requested
                for (int i = 0; line != null && i <= vertices_num; i++) {
                    line = br.readLine();
                    String edge_values[] = line.split(",");
                    for (int j = 0; j <= vertices_num; j++) {
                        pheromone_matrix[i][j] = Double.parseDouble(edge_values[j]);
                    }
                }
        }
        String file_content = convertArrayToString(vertices_num, pheromone_matrix);     //return the pheromone array converted to string
        return file_content;
    }

    /*
        Convert a 2d Array of double into a string to write into a file in readable format
        @param: num_vertices Number of vertices in the graph
        @param: matrix - 2d array of double type
        return: matrix converted to a string
     */
    private static String convertArrayToString(int num_vertices, double[][] matrix) {
        StringBuilder content_sb = new StringBuilder();
        for (int i = 0; i <= num_vertices; i++) {
            for (int j = 0; j <= num_vertices; j++) {
                content_sb.append(String.valueOf(matrix[i][j]));
                content_sb.append(",");
            }
            content_sb.append("\n");
        }
        return content_sb.toString();
    }

    /*
        print the optimal path and distance values on console
     */
    private static void printResults() {
        System.out.println("Optimal distance:" + result_optimal_distance + "\nOptimal path:" + result_optimal_path);
    }

    /*
        Main function of the program : driver function
     */
    public static void main(String[] args) throws Exception {

        //Commandline Arguments(order): input directory path, output directory path, graph_filename, number of vertices, number of iterations
        String input_path = args[0];                                                //Initialise commandline arguments values
        String output_path = args[1];
        String graphFileName = args[2];
        int vertices_num = Integer.parseInt(args[3]);
        int iteration_total = Integer.parseInt(args[4]);
        Boolean jobrunningstat = true;                                              //to check job status after every job iteration
        String pheronomefilename = "try/set.txt";                                   //Initial pheromone data in the file
        String pheromone_data = "";
        int num_mappers = new File(args[0]).listFiles().length;                     //number of mappers = number of input splits
        String graphdata = readAllFileContentToString(graphFileName, vertices_num); //adjacency matrix initialised
        int iteration_count = 1;                                                    //Initialise the current iteration number

        //Start the timer
        long startTime = System.currentTimeMillis();
        while (jobrunningstat && iteration_count <= iteration_total) {              //Chain of mappers and reducer jobs
            if (iteration_count > 1) {
                pheronomefilename = args[1] + (iteration_count - 1) + "/part-00000";
            }
            //create the pheromone data from the output file of previous reduce function
            pheromone_data = createPheromoneData(pheronomefilename, vertices_num, false);
            if (iteration_count > 1)     //print optimal results after every iteration on console
                printResults();

            //Create the job to run map-reduce tasks
            JobConf conf = new JobConf(ACOTsp.class);
            conf.setJobName("ACOTsp");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapperClass(MapTsp.class);                              //Mapper class
            conf.setCombinerClass(Combine.class);                           //Combiner class
            conf.setReducerClass(Reduce.class);                             //reducer class
            conf.setInputFormat(TextInputFormat.class);                     //Input format - text
            conf.setOutputFormat(TextOutputFormat.class);                   //Output format - text
            FileInputFormat.setInputPaths(conf, new Path(input_path));     //input directory name
            FileOutputFormat.setOutputPath(conf, new Path(output_path + iteration_count));    //output directory name

            conf.set("verticesnum", String.valueOf(vertices_num));                     //Constants: number of vertices
            conf.set("alpha", "1.0");                                                  //Constants: pheromone determining factor
            conf.set("evaporation", "0.15");                                           //Pheromone evaporation factor
            conf.set("beta", "2.0");                                                   //Constants: distance(visibility) dtermining factor
            conf.set("Q", "4");                                                        //Constant for calculating pheromone delta
            conf.set("graph", graphdata);                                              //adjacency matrix
            conf.set("pheromonedata", pheromone_data);                                 // Pheromone matrix
            conf.set("pheromonefilename", pheronomefilename);                          //filename with Pheromone matrix
            conf.set("optimaldistance", String.valueOf(result_optimal_distance));      //Optimal distance obtained from previopus iteration
            conf.set("optimalpath", result_optimal_path);                              //optimal path of tour from prev iteration
            conf.set("mappersnumnber", String.valueOf(num_mappers));                   //number of mappers spawned
            RunningJob jobstat = JobClient.runJob(conf);                               //Run the job
            jobrunningstat = jobstat.isSuccessful();                                   //Job status
            iteration_count++;
        }
        //End the timer
        long estimatedTime = System.currentTimeMillis() - startTime;
        pheronomefilename = args[1] + (iteration_count - 1) + "/part-00000";
        createPheromoneData(pheronomefilename, vertices_num, true);         //update the optimal results from latest iteration by reading output file
        printResults();                                                                //Print final results on the console
        System.out.println("Time taken (in milliseconds)" + estimatedTime);
    }
}
