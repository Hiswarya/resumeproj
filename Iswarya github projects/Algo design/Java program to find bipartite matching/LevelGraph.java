/**
 * Levelgraph Class that implements the construction of level graph for every phase
 * of the dinitz algorithm for the graph. All the functionality related
 * to the level graph is encapsulated within this class.
 * Created by Iswarya Hariharan on 11/29/2020.
 */
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

public class LevelGraph {

    private Graph graph;
    private int numNodes;

    /*
    Parametrized constructor for creating a level graph with adjacency matrix
    representation with the number of nodes.
    Pre:
    Post:
    */
    public LevelGraph(int numNodes) {
        this.graph = null;
        this.numNodes = numNodes;
    }

    /*
    Function to construct the level graph from the residual graph during every phase of the ford fulkerson algo
    Pre: Residual graph is created and then passed as parameter where, the level graph gets constructed
    Post: Using BFS, only the edges and nodes in each level increment of 1 gets added to the level graph based
          on the residual graph passed as parameter. No reverse edges that go to the source node is added.
          When doing BFS of the nodes, if the sink could be reached, then the levl of the sink is updated.
          If the level of sink is 0 at the end of this function, then the sink is not reachable.
          Hence false is returned, else, true is returned meaning the sink is reachable from the source vertex.
    */
    public boolean constructLevelGraph(ResidualGraph Gf) {
        //Source and sink are at indices 0 and (number of nodes + 1).
        this.graph = new Graph(this.numNodes + 2);
        Queue<Integer> q = new LinkedList<>();
        //The level of all the nodes are stored in the level array
        int[] level = new int[this.numNodes + 2];
        //The level of source node is 0, all tyhe subsequent levels are more than that
        level[0] = 0;
        //array to store if the vertex has been visited or not
        boolean[] visited = new boolean[this.numNodes+2];
        //initially set all vertices visited values to false
        for(int i = 0; i <= numNodes + 1;i++)
            visited[i] = false;
        //adding the source node index to the queue to start off bfs and assign its visited value to true
        q.add(0);
        visited[0] = true;
        //performing bfs until queue is empty
        while( !q.isEmpty() ) {
            int u = q.remove();
            //checking the adjancecny matrix of residual graph and getting the edges that are one level
            // from it and addings those edges and nodes alone and updating the level graph adjacency matrix
            for(int i = 1; i <= this.numNodes+1 && u != this.numNodes + 1 ; i++) {
                if(Gf.residualGraph.getFlowBetweenVertices(u, i) == 1 && (level[i] == 0 || level[i] == level[u] + 1)) {
                    if(level[i] == 0) {
                        q.add(i);
                        visited[i] = true;
                        level[i] = level[u] + 1;
                    }
                    this.graph.setFlowBetweenVertices(u,i,1);
                }
            }
        }
        //If the sink node was visited during the bfs, then there is path from
        // source to sink so true is returned. else false is returned
        if(visited[this.numNodes + 1])
            return true;
        return false;
    }

    /*
    The function performs the actual dinitiz algorithm on the level graph representation with the adjancency matrix
    pre: The level graph is constructed using bfs from the residual graph
    Post: The dinitiz algorithm finds the possible routes/paths from the source to the edge and augments those paths
          in the residual graph. The function returns the number of such paths obtained in this phase/execution of dinitz algorithm
     */
    public int performDinitzAlgorithm(ResidualGraph Gf) {
        //Initially set location to source
        int currentLocationNode = 0;
        ArrayList<Integer> path = new ArrayList<Integer>();
        //variable that stores the number of paths from source to sink obtained from the level graph when executing this phase of algorithm
        int numPaths = 0;
        //Initially get the next node to the source node index - 0
        int nextNeighborNodeToSource = this.getNextNeighbour(0);
        //Until the next node is not source/not getting stuck at source, continue the loop
        while(nextNeighborNodeToSource != 0) {
            //If the location of current node is sink, then destination has been reached and the path has been found
            if(currentLocationNode == this.numNodes + 1) {
                //augment flow of path in Gf - residual graph and update gf
                Gf.augmentGraphFlow(path);
                //Increase the number of paths obtained from source to destination
                numPaths++;
                //pathsLists.add(path);
                int pathSize = path.size();
                //make flow 0 and remove those edges in levelgraph
                for(int i = 0; i < pathSize - 1; i++) {
                    this.graph.setFlowBetweenVertices(path.get(i), path.get(i+1), 0);
                }
                //start at source once again and initilise path to a new empty list
                currentLocationNode = 0;
                path = new ArrayList<>();
            }
            else {
                //If stuck at a node, then retreat by deleting the incoming edges
                int nextNeighbour = getNextNeighbour(currentLocationNode);
                if(nextNeighbour == 0) {
                    //stuck, so retreat
                    int pathSize = path.size();
                    int previousLocationNode = path.get(pathSize - 2);
                    path.remove(pathSize - 1);
                    //all the incoming edges to the node removed
                    for( int i = 0; i <= this.numNodes + 1; i++) {
                        if(this.graph.getFlowBetweenVertices(i, currentLocationNode) == 1)
                            this.graph.setFlowBetweenVertices(i, currentLocationNode, 0);
                    }
                    //set the location to the previous node and contnue to see if there is another path
                    currentLocationNode = previousLocationNode;
                }
                else {
                    //If the first node is getting added to the path, then add the source node first
                    if(path.isEmpty())
                        path.add(0);
                    //If there is possibility of going to the next node, then add it to the path
                    path.add(nextNeighbour);
                    currentLocationNode = nextNeighbour;
                }
            }
            //If current node is 0, find the next node to source if something exists which can be traversed to create a path
            if(currentLocationNode == 0){
                nextNeighborNodeToSource = getNextNeighbour(0);
            }
        }
        return numPaths;
    }

    /*
    Function to get the next node index from that node index passed as parameter
    if there is flow of 1 between the vertices.
    Pre: The level graph is construced from the residual graph
    Post: The next node index to which this node has a flow/edge to is returned. If no such node is there, then 0 is returned
     */
    private int getNextNeighbour( int nodeIndex) {
        //Get the next neighbour from the given nodeindex except the source node at index=0
        for( int i = 1; i<= this.numNodes + 1; i++) {
            if(this.graph.getFlowBetweenVertices(nodeIndex, i) == 1)
                return i;
        }
        return 0;
    }
}
