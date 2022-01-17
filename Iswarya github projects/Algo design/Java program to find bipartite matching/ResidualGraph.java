/**
 * This class ResidualGraph implements the residual graph which initilally is constructed to store the edges
 * between vertices indices in the adjacency matrix representation with the capacities
 * as all are in forward direction. After the augmentation of phases start,
 * every value of 1 between two vertices in the adjacency matrix represents the
 * flow in that direction from [vertex id in row --> vertex id in column],
 * and this means the flow=0 in opposite direction of [vertex id in column] --> [vertex id in row]
 * Created by Iswarya Hariharan on 11/25/2020
 */
import java.util.ArrayList;
import java.util.HashMap;

public class ResidualGraph {

    public Graph residualGraph;
    private int numNodes;
    // array that stores the matchings. The way matchings are maintained in this array
    // is index represents the left set vertex and value for that index is the matching
    // right set vertex index for that node index
    private int[] matchingsIndices;

    /*
    Parametrized constructor to create and initialize the
    Residual graph object with the required number of nodes
    Pre: The original bipartite graph is created and number of nodes in the bipartite graph is passed
    Post: An adjacency matrix representation with number of nodes + 2 size is created
            as index=0 denotes source and index = numNodes + 1 indicates the sink
    */
    public ResidualGraph(int numNodes) {
        residualGraph = new Graph(numNodes + 2);
        this.numNodes = numNodes;
        this.matchingsIndices = new int[numNodes/2 + 1];
    }

    /*
    Function to construct the initial residual graph from the bipartite graph object
    Pre: The bipartite graph adjacency matrix representation is already created/initialized
    Post: Residual graph adjacency matrix is initialized/constructed by adding the edges
        from source (index=0) to all nodes on left set and
        from all nodes on right set to sink(index = total number of nodes +1).
        Also, for all the other edges in bipartite graph is set to residual graph also.
        So, at the end of construction of residual graph, it would represent the capacities
        along the edges from vertex id on row to vertex id on column.
        The flow along the edges is initially = 0 along all edges in forward direction.
    */
    public void constructResidualGraph( Graph bipartiteGraph ) {
        for(int i = 1; i <= numNodes/2; i++) {
            this.residualGraph.setFlowBetweenVertices(0, i, 1);
            this.residualGraph.setFlowBetweenVertices(i + numNodes/2, numNodes+1, 1);
        }
        //The edge values for all other edges in adjacency matrix in original bipartite graph
        // is represented on residual graph also
        for(int i=1; i <= numNodes; i++) {
            for(int j = 1; j <= numNodes; j++) {
                this.residualGraph.setFlowBetweenVertices(i, j, bipartiteGraph.getFlowBetweenVertices(i,j));
            }
        }
    }

    /*
`   Function to augment the path  on residual graph of edges sent as a parameter
    Pre: the residual graph is constructed to indicate the capacities along the edges
    post: For the edge path passed in a list, the direction of edges along the path
          is reversed (aka: augmented) such that after the augmentation, if an edge has value = 1,
          then the flow is along that direction of [vertex id on row] --> [vertex id on column].
          This means that the flow=0 for the same pair of vertices ,
          [vertex id on column] -> [vertex id on row].
          After augmentation of paths begin, no more the adjacency matrix represents the capacities
          along edges, it represents the flow - as either 0 or 1.
          Here, if an edge in the path flows along left set of nodes to right set of nodes,
          then they are added to the matching array
     */
    public void augmentGraphFlow(ArrayList<Integer> edgePath) {
        int edgePathSize = edgePath.size();
        for(int i = 0; i < edgePathSize - 1; i++) {
            int u = edgePath.get(i);
            int v = edgePath.get(i+1);
            this.residualGraph.setFlowBetweenVertices(u, v, 0);
            this.residualGraph.setFlowBetweenVertices(v, u, 1);
            // If there is an edge in the path from left set of vertices to the right set of vertices,
            // then its added to the appropriate matching
            if(v >= this.numNodes/2 + 1 && v <= this.numNodes && u >= 1 && u <= this.numNodes/2 ) {
                matchingsIndices[u] = v;
            }
        }
    }

    /*
    Function that gives the matched vertex index for the given/passed vertex index(left set vertex) as parameter
    Pre: The residual graph is augmented with all the paths from source to sink
        and there are no more paths from source to sink possible to augment
    Post: The index of the matched vertex to the leftset vertex index passed is returned
     */
    public int getMatchingVertexIndex(int leftSetVertexIndex) {
        return this.matchingsIndices[leftSetVertexIndex];
    }
}
