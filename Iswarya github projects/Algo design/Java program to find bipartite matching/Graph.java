import java.util.Arrays;

/**
 * Class that stores the adjacency matrix representation of a graph.
 * It has the 2d array that stores the adjacency matrix for the graph
 * Created by Iswarya Hariharan on 11/25/2020.
 *
 */
public class Graph {
    //2D int array to store the adjancency matrix representation for the graph object
    private int[][] adjacencyMatrix;
    /*
    Parametrized constructor that initialised a 2d array with size as n*n
    where n is the number of nodes in the graph as passed.
    Pre: When the number of nodes is known and passed as parameter
    Post: A 2d int array representing the value of a cell as 1
          if edge exists from row-index vertex to col-index vertex.
    */
    public Graph( int numNodes ) {
        this.adjacencyMatrix = new int[numNodes][numNodes];
    }

    /*
    Getter method to obtain the value of an edge whether it indicates
        flow or capacity between the node x to node y
    Pre: The adjacency matrix for the graph is created.
    Post: The value of the edge between the vertices x to y is returned/obtained,.
     */
    public int getFlowBetweenVertices(int x, int y) {
        return this.adjacencyMatrix[x][y];
    }

    /*
    Setter method to set the value of an edge indicating capacity or flow
        between tow nodes x and y with the passed value
    Pre: The adjacency matrix for the graph is already set/initiliased with 0 along all edges.
        The passed x and y vertices ids are within 0 and total number of nodes-numNodes of the
        adjacency matrix size on rows and columns.
    Post: The passed value for the edge between x to y vertices is set to that cell in the
          adjacency matrix where, representing the value of a cell as 1 if edge exists
          from row-index vertex to col-index vertex.
    */
    public void setFlowBetweenVertices(int x, int y, int flow) {
        this.adjacencyMatrix[x][y] = flow;
    }

}
