package it.unipi.hadoop;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

public class Centroid extends Point {
    private IntWritable index; //index is the id of centrois

    // Default constructor
    Centroid() {
        super();
        this.index = new IntWritable(0);
        this.clusterPoints = new IntWritable(0);
    }

    // Constructor that takes cords as a parameter
    Centroid(List<DoubleWritable> cords) {
        super(cords, new IntWritable(0));
        this.index = new IntWritable(0);
    }

    // Constructor that takes dimension as a parameter
    Centroid(int dimension) {
        super(dimension);
        this.index = new IntWritable(0);
        this.clusterPoints = new IntWritable(0);
    }

    // Copy constructor, it is to compare later the centroids 
    Centroid(Centroid centroid) {
        super(centroid.getCords(), centroid.getclusterPoints());
        setIndex(centroid.getIndex());
    }

    // Constructor that takes cords, index, and points counter as parameters
    Centroid(List<DoubleWritable> cords, IntWritable index, IntWritable clusterPoints) {
        super(cords, clusterPoints);
        this.index = new IntWritable(index.get());
    }

    // Read the state of the object from DataInput stream
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        index = new IntWritable(dataInput.readInt());
    }

    // Serialize the object's state into a DataOutput stream
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        dataOutput.writeInt(index.get());
    }

    // Check if the centroid has converged based on the threshold and another centroid
    boolean converged(Centroid centroid, Double threshold) {
        return threshold > this.calculateDistance(centroid);
    }

    // Compare this centroid with another centroid
    @Override
    public int compareTo(@Nonnull Centroid centroid) {
        if (this.getIndex().get() == centroid.getIndex().get()) {
            return 0;
        }
        return 1;
    }
    // Return a string representation of the centroid
    public String toString() {
        return this.getIndex() + " " + super.toString();
    }

    // division between the sum of the cords contained in a centroid and their number
    void calculateCentroid() {
        for (int i = 0; i < this.getCords().size(); i++) {
            Double centroid = this.getCords().get(i).get() / clusterPoints.get();
            this.getCords().set(i, new DoubleWritable(centroid));
        }
    }

    // Getter for the index
    IntWritable getIndex() {
        return index;
    }

    // Setter for the index
    void setIndex(IntWritable index) {
        this.index = new IntWritable(index.get());
    }
}

