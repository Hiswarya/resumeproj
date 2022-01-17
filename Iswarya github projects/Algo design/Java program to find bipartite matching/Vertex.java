/**
 * This class Vertex stores the id and name of a vertex
 * This is as per the names given on the input file.
 * Created by Iswarya Hariharan on 11/25/2020.
 */

public class Vertex {

    //Name and vertex id or node id is stored
    private String name;
    private int vertexIndex;

    /*
    Parametrized constructor to create an object of Vertex class
    by initiliazing the id and name of the vertex
    Pre: The name of the node/vertex is read from input file
        and the index of vertex is passed
    Post: Vertex object is created with the passed parameters
     */
    public Vertex(int vertexIndex, String name){
        this.vertexIndex = vertexIndex;
        this.name = name;
    }

    /*
    Getter method for the name value of vertex
    Pre: The name of the vertex object is already set
    Post: The name of the vertex is obtained
     */
    public String getName() {
        return this.name;
    }
}
