package it.unipi.hadoop;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


// mapper input key is object and value is text, output key is centroid and value is a point 
public class KMeansMapper extends Mapper<Object, Text, Centroid, Point> {
    private List<Centroid> centroids = new ArrayList<Centroid>();  // create empty list of tpe centroid

    // context is an object allows map function to write actual intermediate data
    // conf provides some args to be passed from cmd

    //setup invoked once before the map function is called for each input split
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {  
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path centroidsPath = new Path(conf.get("centroidsPath")); //reads this file "centroids/centroids.seq"
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroidsPath, conf);

        Centroid value = new Centroid();
        IntWritable key = new IntWritable();

        while (reader.next(key, value)) {
            // Create a new centroid based on the value and add it to the list
            Centroid centroid = new Centroid(value.getCords()); //create the cordinates
            centroid.setclusterPoints(new IntWritable(0)); //create the points counter (numbers of points associeted to the centroid)
            centroid.setIndex(key); //create centroid ID
            centroids.add(centroid);
        }
        reader.close();
    }


    // context is an object allows map function to write actual intermediate data
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String dataLine = value.toString(); //read file line by line and put into string
        StringTokenizer tokenizer = new StringTokenizer(dataLine, ","); //tokenize each line into n cordinates
        List<DoubleWritable> cords = new ArrayList<DoubleWritable>();

        while (tokenizer.hasMoreTokens()) {
            // Parse each token as a DoubleWritable and add it to the cords list
            cords.add(new DoubleWritable(Double.parseDouble(tokenizer.nextToken()))); //add to the cordinates list
        }

        // Create a new point using the extracted cords
        Point point = new Point(cords); //instatied new point with constructor

        Centroid closestCentroid = null; //  null means no closest centroid has been determined yet 
        Double minDist = Double.MAX_VALUE; // Double.MAX_VALUE a constant in Double class represents maximum value that a double can hold.
        Double tempDist = null; //distnce used in the loop holding the distance between point and centroid

        for (Centroid centroid : centroids) {
            // Calculate the distance between the current centroid and the point
            tempDist = centroid.calculateDistance(point);  //method from centoid class
            if (tempDist < minDist) {
                closestCentroid = centroid;
                minDist = tempDist;
            }
        }

        // Emit the nearest centroid and the point as output
        context.write(closestCentroid, point);
    }
}
