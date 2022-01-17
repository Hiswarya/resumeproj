/**
 * Implementation of reading the set of points and the x and y coordinates from input file
 * and creating an array of objects for them and calling the appropriate methods to find the
 * closest distance between any two pairs using DIVIDE AND CONQUER approach
 * in O(nlogn) time complexity.
 * Created by Iswarya Hariharan and Sneha Manchukonda on 11/4/2020.
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;


public class PointsHolder {
    //List to store the points objects
    private ArrayList<Point> pointsList;
    //Total Number of points
    private int numPoints;
    //Name of input file
    private String inputFile;
    //I/O reader to read the input file
    private BufferedReader fileReader;

    /*
    * Parameterized constructor with input filename passed
    * pre: Input file is a .txt file that should be present in level of src directory
    * post: The numPoints variable is initialized by reading the input file's first line
    *       The points objects array is created to a size of the number of points read
    */
    public PointsHolder(String inputFileName) {

        if (inputFileName == null) {
            this.inputFile = inputFileName;
        } else {
            this.inputFile = inputFileName;
        }
        try {
            this.fileReader = new BufferedReader(new FileReader(this.inputFile));
            numPoints = Integer.parseInt(fileReader.readLine());
            pointsList = new ArrayList<Point>(numPoints);

        } catch (IOException ie) {
            ie.printStackTrace();
        }
        readInputFile();
    }

    /*
    * Function to read the input file line by line and create the Points objects array
    * pre: Input file is a .txt file that should be present in level of src directory
    * post: The points objects are initialised by reading the input filename passed
    */
    private void readInputFile() {
        if(inputFile == null) {
            pointsList = null;
        } else {
			try {
				LineNumberReader reader = new LineNumberReader(new FileReader(inputFile));
				int i = 0;
            // Looping to get the x and y coordinates of each point from the input file
            while (i <= numPoints) {
                String line = reader.readLine();
                if (line != null && reader.getLineNumber() >= 2) {
                    //read each line for both x and y coordinate values by trimming the spaces
                    String[] str = line.trim().split("\\s+");
                    Double x = Double.parseDouble(str[0]);
                    Double y = Double.parseDouble(str[1]);
                    //A new point object is initialized with the x and y coordinate values
                    Point p = new Point(x, y);
                    pointsList.add(p);
                }
                i++;
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
    * Private Function that creates an instance of a class that computes the closest distance
    * among the points using divide and conquer in O(nlogn) time
    * pre: The arraylist of points objects is initiliazed by reading the input file
    * post: The closest distance between any pair of points is computed and printed on console
    */
	private void computeClosestDistanceDiveAndConquer() {
		if (pointsList.size() != numPoints) {
			System.out.println("The number of points and the number of coordinates values given in file are not equal.");
		} else {
			ClosestPoints closestPoints = new ClosestPoints(this.pointsList, this.numPoints);
			closestPoints.executeDivideAndConquerClosestPoints();
		}
	}

    public static void main(String args[]) {
        PointsHolder pointsHolder = new PointsHolder("program2data.txt");
        pointsHolder.computeClosestDistanceDiveAndConquer();
    }
}