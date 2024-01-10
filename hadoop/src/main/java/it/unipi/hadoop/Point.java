package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.commons.logging.Log;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.commons.logging.LogFactory;
import java.util.List;
import java.util.ArrayList;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import javax.annotation.Nonnull;


//the Point class implements the WritableComparable interface with the type parameter Centroid, 
//indicating that it can be serialized and compared with other objects of the Centroid class. 
//The compareTo() method is implemented to return 0, which means the Point objects are considered equal in the current implementation.
public class Point implements WritableComparable<Centroid> {     //writableCombarable is a java interface used for comparing and sorting
    private List<DoubleWritable> cords; // List to store the cords of the point
    protected IntWritable clusterPoints; // Counter to keep track of the number of points associated with this instance

    // Default constructor
    Point() {
        cords = new ArrayList<DoubleWritable>();  //new empty ArrayList object is created
        this.clusterPoints = new IntWritable(0);  //holds an initial value of 0
    }

    // Constructor that takes a list of cords and points counter as parameters
    // clusterPoints is the number of point associated to the centroid
    Point(List<DoubleWritable> cords, IntWritable clusterPoints) {
        this.cords = new ArrayList<DoubleWritable>();  //This creates an empty list to store the coordinates of a point.
        for (DoubleWritable cord : cords) {     //loop that iterates over each element in the cords list
            this.cords.add(new DoubleWritable(cord.get()));  //copy each coordinate from original list and adds it to the new cords list.
        }
        this.clusterPoints = new IntWritable(clusterPoints.get());
    }

    // Constructor that takes the dim of the point as a parameter
    //allows the Point object to have a list of coordinates with the desired dimension and
    // an initial value of 0 for the cluster points counter.
    Point(int dim) {
        cords = new ArrayList<DoubleWritable>(dim);
        for (int i = 0; i < dim; i++) {
            cords.add(new DoubleWritable(0));
        }
        this.clusterPoints = new IntWritable(0);
    }
    
    // Constructor that takes a list of cords as a parameter (assuming a single point)
    // It is constructer for single/each point
    Point(List<DoubleWritable> cords) {
        this.cords = new ArrayList<DoubleWritable>();  //each Point object has its own separate list for coordinates.
        for (DoubleWritable cord : cords) {
            this.cords.add(new DoubleWritable(cord.get()));
        }
        this.clusterPoints = new IntWritable(1); //sets the clusterPoints counter to 1
    }

    // Returns a string representation of the point
    public String toString() {
        String line = "";
        for (DoubleWritable cord : cords) {
            line += cord.get() + ","; // Concatenate each cord separated by commas
        }
        return line;
    }

    // Returns the list of cords
    public List<DoubleWritable> getCords() {
        return cords;
    }

    // Calculates the Euclidean distance between this point and another given point 
    // iterates over the coordinates of the two points, calculates the squared differences
    public Double calculateDistance(Point point){
        Double sum = 0.0;
        for (int i = 0; i < point.getCords().size(); i++) {
            Double centroid_cords = this.getCords().get(i).get();
            Double point_cords = point.getCords().get(i).get();
            sum += Math.pow(centroid_cords - point_cords,  2);
        }
        return Math.sqrt(sum);
    }

    // Serializes the point object's state into a DataOutput stream
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(cords.size()); // Write the size of the cords list
        for (DoubleWritable cord : cords) {
            dataOutput.writeDouble(cord.get()); // Write each cord as a double
        }
        dataOutput.writeInt(clusterPoints.get()); // Write the clusterPoints as an integer
    }

    // Deserializes the object's state from a DataInput stream
    public void readFields(DataInput dataInput) throws IOException {
        cords = new ArrayList<DoubleWritable>();
        int dim = dataInput.readInt(); // Read the size of the cords list
        for (int i = 0; i < dim; i++) {
            cords.add(new DoubleWritable(dataInput.readDouble())); // Read each cord as a double
        }
        clusterPoints = new IntWritable(dataInput.readInt()); // Read the clusterPoints as an integer
    }

    //returns the value of the clusterPoints
    IntWritable getclusterPoints() {
        return clusterPoints;
    }

    // update the clusterPoints value of a Point object to reflect changes or modifications in the associated cluster points count
    void setclusterPoints(IntWritable clusterPoints) {
        this.clusterPoints = new IntWritable(clusterPoints.get());
    }
}
