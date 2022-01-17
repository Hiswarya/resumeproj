import java.io.IOException;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * Inversed index with Hadoop Map Reduce.
 * AuthOr: Iswarya Hariharan
 */
public class InvertedIndex {
    /**
     * Mapper for inverted indexing.
     * The base class Mapper is parameterized by
     * <in key type, in value type, out key type, out value type>.
     * Thus, this mapper takes (LongWritable doc_Id, Text value) pairs and outputs
     * (Text key, Text value) pairs. The input keys are assumed
     * to be identifiers for documents, which are ignored, and the values
     * to be the content of documents. The contents are searched for keywords
     * from the JobConf object. The output keys are the keywords for search
     * and the output values are the file name and number of times
     * that word appeared within a document.
     */
    public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text doc_count = new Text();
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
         * pairs for each keyword found in the document.
         *
         * @param docId           Document identifier (ignored).
         * @param Text            of the current document.
         * @param OutputCollector object for accessing output,
         *                        configuration information, etc.
         * @reporter For getting the filename and path
         */

        public void map(LongWritable doc_Id, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            // retrieve # keywords from JobConf
            int argc = Integer.parseInt(conf.get("argc"));
            // get the current file name
            FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
            String filename = "" + fileSplit.getPath().getName();
            //Hashmap for storing keyword and count
            HashMap<String, Integer> keyword_map = new HashMap<String, Integer>();
            String keywrd = "keyword";
            Integer default_val = 0;

            //Add all keywords to the hashmap and intialise count as zero
            for (int i = 0; i < argc; i++) {
                StringBuilder key_num = new StringBuilder();
                key_num.append(keywrd);
                key_num.append(i);
                String keywrd_name = key_num.toString();
                keyword_map.put(conf.get(keywrd_name).trim(), default_val);
            }

            //Tokenize each line in the file and update hashmap if the word is a search keyword
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String word_fromdoc = new String();
            while (tokenizer.hasMoreTokens()) {
                word_fromdoc = tokenizer.nextToken();
                Integer count = keyword_map.get(word_fromdoc);
                if (count != null) {
                    keyword_map.put(word_fromdoc, count + 1);
                }
            }

            //Compute posting : <keyword, filename_count> and collect in output
            Iterator keymap_iterator = keyword_map.entrySet().iterator();
            while (keymap_iterator.hasNext()) {
                Map.Entry pair = (Map.Entry) keymap_iterator.next();
                Integer key_count = (Integer) pair.getValue();
                String keywrd_str = pair.getKey().toString();   //Key word from hashmap
                StringBuilder file_count_str = new StringBuilder();
                file_count_str.append(filename);
                file_count_str.append("_");
                file_count_str.append(key_count);
                String final_doc_count_value = file_count_str.toString();
                doc_count.set(final_doc_count_value);
                word.set(keywrd_str);
                output.collect(word, doc_count);    //Final data sent to collect for each keyword in the file
            }
        }
    }

    /**
     * Reducer for InversedIndexing.
     * <p>
     * Like the Mapper base class, the base class Reducer is parameterized by
     * <in key type, in value type, out key type, out value type>.
     * <p>
     * For each Text key, which represents a word, this reducer gets a list of
     * Text values, computes the count of those values in all documents
     */
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        /**
         * Actual reduce function.
         *
         * @param key             Serach keyword.
         * @param values          Iterator over the text for this key: filename_count.
         * @param OutputCollector object for accessing output and putting values,
         *                        configuration information, etc.
         * @param Reporter        ignored
         */
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            Integer count;
            Text doc_list = new Text();
            String doc_name, value_string;
            HashMap<String, Integer> doc_container = new HashMap<String, Integer>();
            StringBuilder doc_list_str = new StringBuilder();
            //For all values , update the document container - hashmap
            while (values.hasNext()) {
                Text value = values.next();
                value_string = value.toString();
                String[] split_value = value_string.split("_");
                if (split_value.length > 1) {
                    doc_name = split_value[0];
                    count = Integer.parseInt(split_value[1]);
                    //Update doc container map with the obtained value if file name is already present, else add to the container
                    Integer current_count = doc_container.get(doc_name);
                    if (current_count != null) {
                        if (count > 0)
                            doc_container.put(doc_name, (count + current_count));
                    } else {
                        doc_container.put(doc_name, count);
                    }
                }
            }

            // TreeMap to store values of HashMap - custom compare function to sort file names with numbers
            TreeMap<String, Integer> sorted_doc_container = new TreeMap<>(new Comparator<String>() {
                @Override
                public int compare(String key1, String key2) {
                    //parse only text that contains numbers in the file name
                    int k1 = Integer.parseInt(key1.replaceAll("[^0-9]", ""));
                    int k2 = Integer.parseInt(key2.replaceAll("[^0-9]", ""));
                    return Integer.compare(k1, k2);
                }
            });

            // Copy all data from hashMap into TreeMap
            sorted_doc_container.putAll(doc_container);

            //Create the document list string for final output
            Iterator doc_iterator = sorted_doc_container.entrySet().iterator();
            String doc_key, count_val;
            while (doc_iterator.hasNext()) {
                Map.Entry pair = (Map.Entry) doc_iterator.next();
                if (doc_list_str.length() > 0)
                    doc_list_str.append(":");
                doc_key = pair.getKey().toString();
                count_val = pair.getValue().toString();
                System.out.println(doc_key + "  _  " + count_val);
                doc_list_str.append(doc_key);
                doc_list_str.append("_");
                doc_list_str.append(count_val);
            }
            doc_list.set(doc_list_str.toString());
            output.collect(key, doc_list);
        }
    }

    /**
     * Entry-point for the program. Constructs a JobConf object representing a single
     * Map-Reduce job and asks Hadoop to run it. When running on a cluster, the
     * final "runJob" call will distribute the code for this job across
     * the cluster.
     *
     * @param args command-line arguments
     */

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(InvertedIndex.class);
        conf.setJobName("Inverted_Index");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map1.class);                            //Mapper class
        conf.setCombinerClass(Reduce.class);                        //Reducer class
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);                 //Input format-text
        conf.setOutputFormat(TextOutputFormat.class);               //Output format - text

        FileInputFormat.setInputPaths(conf, new Path(args[0]));     //input directory name
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));    //output directory name

        conf.set("argc", String.valueOf(args.length - 2));          // argc maintains #keywords
        for (int i = 0; i < args.length - 2; i++)
            conf.set("keyword" + i, args[i + 2]);                   // keyword1, keyword2, ...
        JobClient.runJob(conf);
    }
}
