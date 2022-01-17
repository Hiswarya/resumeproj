/**
 * Created by Iswarya Hariharan on 11/29/2020.
 *
 * Implementation of the ford-fulkerson algorithm on a bipartite graph
 * to find the maximal matchings. This problem is specific for bipartite graphs
 * where each edge has a capacity of 1 or 0 and thereby the flow is present.
 * The vertices in the graph here uses 1-based indexing as per the input file.
 */
import java.io.*;
import java.util.*;


public class BipartiteMatching {
    //Stores the name of the input file
    private String inputFile;
    //File reader for reading the input file
    private BufferedReader fileReader;
    private int numNodes;
    //List of all vertices objects in the graph as read from the input file
    private ArrayList<Vertex> vertices;
    //This graph object stores the original bipartite graph adjacency matrix
    private Graph bipartiteGraph;
    //To represent the residual graph throughout the program
    private ResidualGraph Gf;

    /*
    Parametrized constructor that initilializes the inputfilename and initializes
    creating objects for all the vertices.
    Pre: The valid input file name is passed from the function that creates this class object
    Post: The number of vertices is read from the file and so many number of vertex objects
          are initiliazed. The required function for processing the reading of the input file
          further is called.
    */
    public BipartiteMatching(String inputFileName) {
        if (inputFileName == null) {
            this.inputFile = inputFileName;
        } else {
            this.inputFile = inputFileName;
        }
        try {
            this.fileReader = new BufferedReader(new FileReader(this.inputFile));
            //The first line contains the number of nodes in the graph
            numNodes = Integer.parseInt(fileReader.readLine());
            vertices = new ArrayList<Vertex>(numNodes);
        } catch (IOException ie) {
            ie.printStackTrace();
        }
        readInputFile();
    }

    /*
    Function to read the input file to get the names of vertices and edges
    between the vertices in the bipartite graph.
    Pre: The constructor reads the input file and has read the first line
        to get the number of nodes
    Post: The vertices names in order are read from the file and the total
        number of edges and between which vertices it is present is read in order
        and the adjacency matrix representation for the bipartite graph is created.
    */
    private void readInputFile() {
        //If the input file name is null then no other functionality for
        // finding the matchings can be done
        if(inputFile == null) {
            vertices = null;
        } else {
            try {
                LineNumberReader reader = new LineNumberReader(new FileReader(inputFile));
                int i = 0;
                // Looping to get the names of all the vertices
                while (i <= numNodes) {
                    String line = reader.readLine();
                    if (line != null && reader.getLineNumber() >= 2) {
                        vertices.add(new Vertex(i-1, line));
                    }
                    i++;
                }
                //The number of edges and between which they go is also read from the file
                int numEdges = Integer.parseInt(reader.readLine());
                int j = 1;
                bipartiteGraph = new Graph(numNodes + 1);
                while(j <= numEdges) {
                    String line = reader.readLine();
                    String[] str = line.trim().split("\\s+");
                    int x = Integer.parseInt(str[0]);
                    int y = Integer.parseInt(str[1]);
                    //Since the edge go from left set to right set of vertices in bipartite graph,
                    //the lesser one is taken out first and edge is placed there
                    if(x>y) {
                        int temp = x;
                        x = y;
                        y = temp;
                    }
                    bipartiteGraph.setFlowBetweenVertices(x, y, 1);
                    j++;
                }
                // Closing the stream after reading the input from the file
                reader.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /*
    Function to find the matchings withe the bipartite graph by constructing the
    residual graph, performing dinitz algorithm and thereby the ford-fulkerson algorithm.
    Finally all the matches are printed.
    Pre: The bipartite graph adjancency matrix representation is set
    Post: ford fulkerson algorithm with residual graph and level graphs is executed to store
          the matchings at every phase as path augmentation is done.
          Finally, the matchings are printed.
     */
    public void findMatchings() {
        Gf = new ResidualGraph(numNodes);
        //residual graph is constructed intially by adding source and sink nodes
        //initially the values in adjacecny matrix indicates the capacities along edges,
        //subsequently after paths augmentation satart, it indicates teh flow along those edges
        Gf.constructResidualGraph(bipartiteGraph);
        //level graph object is initlaised so that it can be used for
        // every phase of dinitz algo
        LevelGraph Lg = new LevelGraph(numNodes);
        //number of paths obtained from each phase of dinitz algo
        int numPaths = 0;
        //Until there is no path from source to sink possible the loop continues
        while(true) {
            Lg.constructLevelGraph(this.Gf);
            numPaths = Lg.performDinitzAlgorithm(this.Gf);
            if(numPaths == 0)
                break;
        }
        //printing the matchings
        printFinalMatchings();
    }

    /*
    The matchings based on the edges added from left to right set during
    path augmentation in residual graph is stored in ResidualGraph class
    object member variable. These are extracted one by one and their
    corresponding names from the vertex objects list is taken out and printed.
    Pre: The ford fulkerson algorithm for the bipartite graph is done.
    Post: the matchings (vertices indices) stored in Residual graph object is
        extracted and printed based on vertices names stored in the vertices list.
        The total number of matches found is also printed on console.
    */
    private void printFinalMatchings() {
        int matchedIndex = 0;
        //To store the number of matchings found
        int numMatchings = 0;
        for(int i = 1; i <= numNodes/2; i++) {
            matchedIndex = Gf.getMatchingVertexIndex(i);
            if(matchedIndex != 0) {
                System.out.println(this.vertices.get(i-1).getName() + " / " + this.vertices.get(matchedIndex-1).getName());
                numMatchings++;
            }
        }
        //To display the total number of matchings
        System.out.println(numMatchings + " total matches");
    }

    /*
    Main function- entry point of the program.
    Pre: input file to read for the bipartite graph data is known
    Post: Input file is passed to bipartite class object and
        appropriate function to find the matchings is called.
    */
    public static void main(String args[]) {
        BipartiteMatching bipartiteMatching = new BipartiteMatching("program3data.txt");
        bipartiteMatching.findMatchings();
    }
}
