package it.unipi.hadoop;

import java.util.HashMap;
import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;

public class KMeansReducer extends Reducer<Centroid, Point, IntWritable, Centroid> { 
    private HashMap<IntWritable, Centroid> oldCentroids = new HashMap<IntWritable, Centroid>(); // A hashmap to store the old centroids
    private HashMap<IntWritable, Centroid> newCentroids = new HashMap<IntWritable, Centroid>(); // A hashmap to store the new centroids

    public enum COUNTER {CONVERGED} // An enum to define a counter for the number of converged centroids 

    
    @Override
    public void reduce(Centroid consideredCentroid, Iterable<Point> points, Context context) throws InterruptedException, IOException{
        Configuration conf = context.getConfiguration(); // Get the configuration object
        Centroid newCentroid = new Centroid(conf.getInt("dim", 2)); // Create a new centroid object with the given dim 
 
        int pointsCount = 0; // A counter to keep track of how many points are assigned to this centroid
        for(Point point : points) { // Iterate over the points that belong to this centroid

            for (int i = 0; i < point.getCords().size(); i++) { // Iterate over the cords of each point
                newCentroid.getCords().get(i).set(newCentroid.getCords().get(i).get() + point.getCords().get(i).get()); // Add the coordinate value to the corresponding coordinate of the new centroid
    //This generate a newCentroid with the sum of the partialSum calculated in the Combiner
            }
            pointsCount += point.getclusterPoints().get(); // Add the number of points represented by this point object to the counter
            //This variable increase with the number of Points inside each partialSum calculated in the Combiner
        }
        newCentroid.setIndex(consideredCentroid.getIndex()); // Set the index of the new centroid to be the same as the old one
        newCentroid.setclusterPoints(new IntWritable(pointsCount)); // Set the number of points assigned to this centroid
        newCentroids.put(newCentroid.getIndex(), newCentroid); // Put the new centroid in the hashmap with its index as the key (Index, Centroid) it will be usefull
        oldCentroids.put(consideredCentroid.getIndex(), new Centroid(consideredCentroid)); // Put the old centroid in the hashmap with its index as the key (Index, Centroid)
    }

    @Override
    protected void cleanup(Context context) throws InterruptedException, IOException { 
        Configuration conf = context.getConfiguration(); // Get the configuration object
        FileSystem fs = FileSystem.get(conf); // Get a file system object
        Path centroidsPath = new Path(conf.get("centroidsPath")); // Get the path where to write the centroids
        Writer.Option fileOption = Writer.file(centroidsPath); // Create an option for writing to a file
        Writer.Option keyClassOption = Writer.keyClass(IntWritable.class); // Create an option for specifying the key class
        Writer.Option valueClassOption = Writer.valueClass(Centroid.class); // Create an option for specifying the value class
 
        Writer writer = SequenceFile.createWriter(conf, fileOption, keyClassOption, valueClassOption); // Create a writer object for writing into files
 
        int k = conf.getInt("k", 2); // Get the number of clusters from the configuration
        Iterator<Centroid> centroidsIterator = newCentroids.values().iterator(); // Get an iterator over the values of the new centroids hashmap
        //An iterator is usefull to lookup all Values in and HashMap with the Method hasNext()

        Centroid newCentroid; // The new considered centroid
        Centroid oldCentroid; // The old centroid with the same index of newCentroid

        Double threshold = Double.parseDouble(conf.get("threshold")); // Get the threshold value for convergence from the configuration
        Double meanDistance = 0.0; // A variable to store the mean distance between old and new centroids

        while(centroidsIterator.hasNext()){ // Loop over all the new centroids
            //hasNext(): gives true if there are elements inside the collection else it gives false 

            newCentroid = centroidsIterator.next(); // Get the next new centroid
            //next(): return the current element of the collection and move to the next one

            newCentroid.calculateCentroid(); // Calculate the centroid cords by dividing the partial sums by the number of points
            oldCentroid = oldCentroids.get(newCentroid.getIndex()); // Get the old centroid with the same index from the hashmap

            meanDistance += Math.pow(newCentroid.calculateDistance(oldCentroid), 2); // Add the squared distance to the mean distance variable
            writer.append(newCentroid.getIndex(), newCentroid); // Write the new centroid to the file with its index as the key
            context.write(newCentroid.getIndex(), newCentroid); // Write the new centroid to the output with its index as the key
        }
 
        writer.syncFs(); // Sync the file system
        writer.close(); // Close the writer
 

        meanDistance = Math.sqrt(meanDistance / k); // Calculate the mean distance by taking the square root of the sum divided by k
        if(meanDistance < threshold){ //the mean distance is less than the threshold
            context.getCounter(COUNTER.CONVERGED).increment(1); // Increment the counter for converged centroids by one
        }
    }
}
