/**
 * This class 'Point' stores the coordinate values of a point on a plane
 * The 2D coordinates are x and y
 * Created by Iswarya Hariharan and Sneha Manchukonda on 10/30/2020.
 */
class Point {

    private double x, y;

    /*
    * Parametrized constructor for initiliasing the x and y coordinates values
    * pre: The x and y coordinates are read from the input file and passed
    * post: Point object for the point is created with the x and y coordinates
    */
    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    /*
    * Getter method for x coordinate value
    * pre: The x coordinate is already set by reading from input file
    * post: The x coordinates are obtained for the Point object
    */
    public double getX() {
        return this.x;
    }

    /*
    * Getter method for y coordinate value
    * pre: The y coordinate is already set by reading from input file
    * post: The y coordinates are obtained for the Point object
    */
    public double getY() {
        return this.y;
    }
}
