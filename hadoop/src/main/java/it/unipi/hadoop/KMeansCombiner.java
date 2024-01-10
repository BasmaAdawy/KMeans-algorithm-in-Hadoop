package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;


// input[key:centroid, value:point], output[key:centroid, value:point]
public class KMeansCombiner extends Reducer<Centroid, Point, Centroid, Point> {
    @Override
    //input[key:centroid, value:list of points] 
    //calculate the partial sum for each key and the count of the points that are assigned to the same centroid
    public void reduce(Centroid centroid, Iterable<Point> points, Context context) throws InterruptedException, IOException {
        Configuration conf = context.getConfiguration(); //retrieves the Configuration object that contains the job configuration settings
        int counter = 0;
        Point partialSum = new Point(conf.getInt("dim", 2)); // Create a new Point object to store the partial sum, if dim not given its 2

        // Iterate over the points associated with the current centroid
        for(Point point: points){ //it is automatically generated key-value data structer where value with the same key are marged together
            // Iterate over the cords of the points
            for(int i=0; i<conf.getInt("dim", 2); i++){
                double partialSumcord = partialSum.getCords().get(i).get(); // Get the current partial sum cord
                double pointcord = point.getCords().get(i).get(); // Get the current point cord
                partialSum.getCords().get(i).set(partialSumcord + pointcord); // Update the partial sum cord
            }
            counter++; // Increment the counter to keep track of the number of points
        }

        partialSum.setclusterPoints(new IntWritable(counter)); // count of points assigned to a particular cluster.
        context.write(centroid, partialSum); // Emit the centroid and the partial sum
    }
}
