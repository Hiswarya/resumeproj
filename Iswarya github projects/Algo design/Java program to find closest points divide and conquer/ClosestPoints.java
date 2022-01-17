/**
 * Implementation of finding the distance between closest pair of points algorithm
 * using DIVIDE AND CONQUER approach in O(nlogn) time complexity.
 * The points x and y coordinate values are read from the input file
 * and the program prints the distance between the closest pair of points
 * in every recursive call at the divide and conquer stage.
 * The last line prints the distance between the closest pair of points
 * in the entire set of points.
 * Authored by Iswarya Hariharan and Sneha Manchukonda on 10/30/2020.
 */

import java.util.*;

class ClosestPoints{
    //List to store the sorted list of points array indices on x and y coordinates
    private ArrayList<Integer> Px, Py;
    //List to store the points objects
    private final ArrayList<Point> pointsList;
    //Total Number of points
    private final int numPoints;

    /*
    * Parameterized constructor with list of points objects passed and the total number of points
    * pre: The array of Points objects is already initiliazed and passed along with the number of points
    * post: The numPoints variable is set for this object and the
    *       array of points objects is set or references to the arraylist passed to pointsList member variable
    */
	public ClosestPoints(ArrayList<Point> pointsList, int numPoints) {

		this.pointsList = pointsList;
		this.numPoints = numPoints;

	}

    /*
    * Function that calls other private functions that executes the divide and conquer of subproblems to find the closest distance
    * The sorted array on x and y coordinates is computed and stored which is used for further divide and conquer subproblems
    * pre: The array of Points objects is already set and the number of points is set
    * post: The sorted arrays Px and Py on x and y coordinates repectively are created
    *       and the divide and conquer on the Px array is called/inititiated
    */
	public void executeDivideAndConquerClosestPoints() {
		if (pointsList == null) {
			System.out.println("The given input file is null");
		} else {
			createSortPointsCoordinates();
			findClosestPoints(0, numPoints - 1, Py);
		}
	}

    /*
    * Function to create an arraylist that stores the indices of the Points objects
    * pre: The PointsList array of 'Point' objects is initiliased by reading the input file
    * post: The indices of the PointsList arraylist is stored for sorting on its x and y coordinates
    */
    private ArrayList<Integer> createPointsIndices() {
        ArrayList<Integer> pointsIndices = new ArrayList<Integer>(numPoints);
        for (int i = 0; i < numPoints; i++) {
            pointsIndices.add(i);
        }
        return pointsIndices;
    }

    //custom Comparator that accesses the index of a Point object from the PointsList
    // and sorts on its x-coordinate values in in O(nlogn) complexity(java inbuilt merge sort)
    Comparator<Integer> xIndicesSortComparator = new Comparator<Integer>() {
        public int compare(Integer index1, Integer index2) {
            return Double.compare(pointsList.get(index1).getX(), pointsList.get(index2).getX());
        }
    };

    //custom Comparator that accesses the index of a Point object from the PointsList
    // and sorts on its y-coordinate values in O(nlogn) complexity(java inbuilt merge sort)
    Comparator<Integer> yIndicesSortComparator = new Comparator<Integer>() {
        public int compare(Integer index1, Integer index2) {
            return Double.compare(pointsList.get(index1).getY(), pointsList.get(index2).getY());
        }
    };

    /*
    * Function to create two arraylists Px and Py that stores the indices of the Points objects
    * and has them sorted on x and y coordinates respectively using custom comparators defined
    * pre: The PointsList array of 'Point' objects is initiliased by reading the input file
    * post: The sorted indices of the PointsList arraylist on x and y-coordinates
    *       are stored in two different arraylists Px and Py respectively
    */
    private void createSortPointsCoordinates() {
        Px = createPointsIndices();
        Py = createPointsIndices();
        Collections.sort(Px, xIndicesSortComparator);
        Collections.sort(Py, yIndicesSortComparator);
    }

    /*
    * Function to compute the Euclidean distance between two points
    * pre: Two point objects are passed to obtain the distance between them
    * post: The Euclidean distance formula by accessing the two Point objects x and y coordinates
    *       is computed and returned
    */
    private double getEuclideanDistance(Point p1, Point p2) {
        return(Math.sqrt(Math.pow(p2.getX() - p1.getX(), 2) + Math.pow(p2.getY() - p1.getY(), 2)));
    }

    /*
    * Function involving the recursive call dividing the points base case which computes the distance between
    * the points if they are 3 or lesser by looping through them and finding
    * the euclidean distance between them
    * pre: The start and end indices of the Px array(sorted points on x coordinates)
    * is passed to obtain the closest points if the difference between them is 3 or lesser
    * post: The Euclidean distance between the closest points within the indices passed is returned
    */
    private double bruteForceComputeDistance(int Px_start, int Px_end) {
        double minDistance = Double.MAX_VALUE;
        for(int i = Px_start; i < Px_end; i++) {
            //Finds the closest distance between the points using euclidean distance calculation
            for(int j = i+1; j <= Px_end; j++) {
                double distance = getEuclideanDistance(pointsList.get(Px.get(i)), pointsList.get(Px.get(j)));
                if(distance < minDistance) {
                    minDistance = distance;
                }
            }
        }
        //Print the min-distance between the indices of the Px array
        printFinalDistance(Px_start, Px_end, minDistance);
        return minDistance;
    }

    /*
    * Function that finds the least of the distances passed
    * pre: The left and right subproblem's distances are passed to obtain the least out of these
    * post: The least distance of the two distance values passed is returned
    */
    private double findClosestPointsDelta(double c1, double c2) {
        if(c1 < c2) {
            return c1;
        }
        return c2;
    }

    /*
    * Function that computes the closest distance between the set of points in the 'strip' arraylist
    * If there is a pair of points in the strip that has lesser distance than the delta(already minimum distance)
    * then the new minimum distance is returned, else the delta distance itself if returned
    * pre: The set of points indices in the stripPoints arraylist is passed along with the delta
    *      distance of the right and left subproblems.
    * post: The least distance of closest points within the strip and the delta distance received is returned
    */
    private double computeClosestPointsInStrip(ArrayList<Integer> stripPoints, double deltaDistance) {
        double minDistance = deltaDistance;
        //This will only compare maximum of 7 points from each point
        for(int i = 0; i < stripPoints.size(); i++) {
            //If the points distance being compared is already exceeded delta distance in y-coordinates
            // then there is no need to compute the euclidean distance
            for( int j = i + 1; j < stripPoints.size() && Math.abs(pointsList.get(stripPoints.get(j)).getY() - pointsList.get(stripPoints.get(i)).getY()) < deltaDistance; j++) {
                double distance = getEuclideanDistance(pointsList.get(stripPoints.get(j)), pointsList.get(stripPoints.get(i)));
                if(distance < minDistance) {
                    minDistance = distance;
                }
            }
        }
        return minDistance;
    }

    /*
    * Function that prints the indices of the sorted xcoordinates array and the closest distance passed
    * pre: The start and end indices are passed along with the closest distance to be printed
    * post: The distance and the start and end indices are printed
    */
    private void printFinalDistance(int Px_start, int Px_end, double distance) {
        String roundedDouble = String.format("%.4f", distance);
        System.out.println("D[" + Px_start + "," + Px_end + "]: " + roundedDouble);
    }

    /*
    * Function that gets called recursively and implements the left and right subproblems
    * and computes the min distance 'delta' of these two subproblems.
    * The points on a strip of distance 'delta' is taken out and if any closer points
    * among them lesser than delta is returned.
    * pre: The start and end indices of sorted xcoordinate array Px is passed and the list of
    *      sorted y-coordinates arraylist 'Py' is also passed.
    *      The list of points objects are initilaised and sorted Px array on x-soordinates and
    *      sorted Py array on y-coordinates are already created.
    * post: The least distance of each suproblem is returned to the calling function.
    */
    private double findClosestPoints(int Px_start, int Px_end, ArrayList<Integer> Py) {
        //if there are 3 or lesser points in the problem, then brute force computation is done
        if(Px_end - Px_start <= 2) {
            return bruteForceComputeDistance(Px_start, Px_end);
        }        
        //The mid index value between the indices of Px array's start and end indices
        int midpoint = (Px_end - Px_start) / 2;
        //The midpoint line value(on x axis) that divides equal number of points and 
        //is inbetween the mid indices for left and right subproblems is computed
        double midpointLine;       
        midpointLine = (pointsList.get(Px.get(Px_start + midpoint)).getX() + pointsList.get(Px.get(Px_start + midpoint + 1)).getX()) / (double)2;
        //Lists that store the y-coordinate sorted points objects indices for the left and right subproblems
        ArrayList<Integer> PyLeft = new ArrayList<Integer>();
        ArrayList<Integer> PyRight = new ArrayList<Integer>();
        //If the point's x coordinate lies to the left of the midpoint line then
        // the y-coordinate of the point is put into Pyleft, else Pyright
        for(int i = 0; i < Py.size(); i++) {
            if(pointsList.get(Py.get(i)).getX() <= midpointLine)
                PyLeft.add(Py.get(i));
            else
                PyRight.add(Py.get(i));
        }
        //Closest points distances on left and right subproblems is computed by
        // recursive function call by divide and conquer method
        double leftSubProblem = findClosestPoints(Px_start, Px_start + midpoint, PyLeft);
        double rightSubProblem = findClosestPoints(Px_start + midpoint + 1, Px_end, PyRight);
        //Minimum of the left and right sub problems distance is obtained as delta
        double deltaClosestPoints = findClosestPointsDelta(leftSubProblem, rightSubProblem);
        //List that stores the indices of points on the strip of delta distance from the midpointline
        ArrayList<Integer> stripPointsIndices = new ArrayList<Integer>();
        //compute the strip points
        for(int i = 0; i < Py.size(); i++) {
            if(Math.abs(pointsList.get(Py.get(i)).getX() - midpointLine) < deltaClosestPoints)
                stripPointsIndices.add(Py.get(i));
        }
        //call function to compute if there is a pair of points in the strip having distance lesser than delta
        double stripClosestPoints = computeClosestPointsInStrip(stripPointsIndices, deltaClosestPoints);
        if(stripClosestPoints < deltaClosestPoints) {
            deltaClosestPoints = stripClosestPoints;
        }
        //the least obtained distance between the pair of points on sorted x-coordinates points is printed
        // and returned to calling function
        printFinalDistance(Px_start, Px_end, deltaClosestPoints);
        return deltaClosestPoints;
    }

}