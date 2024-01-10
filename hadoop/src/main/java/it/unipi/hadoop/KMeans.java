package it.unipi.hadoop;

import java.io.*;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Random;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Reader;


public class KMeans
{
    // Method to generate random centroids
    private static void generateRandomCentroids(Configuration conf, int k, int dim, int instances, Path centroidsPath, String fileName) throws IOException{
        FileSystem fs = FileSystem.get(conf);

        Writer.Option fileOption = Writer.file(centroidsPath); //read the file
        Writer.Option keyClassOption = Writer.keyClass(IntWritable.class); //write the key (IntWritable) to file
        Writer.Option valueClassOption = Writer.valueClass(Centroid.class); //write the value (Centroid) to key

        Writer writer = SequenceFile.createWriter(conf, fileOption, keyClassOption, valueClassOption); //This line creates a SequenceFile.Writer object using the provided configuration, file option, key class option, and value class option. The writer object is used to write data to the sequence file.

        try{
            Random generator = new Random();
            for(int index = 0; index < k; index++){
                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String line = null;
                int linesToSkip = (generator.nextInt(instances-1));
                // Skip lines in the file randomly
                for(int i = 0; i <= linesToSkip; i++){
                    line = reader.readLine();
                }

                // Split the line into coordinate strings
                String[] cordsString = line.toString().split(",");
                List<DoubleWritable> cords = new ArrayList<DoubleWritable>();

                // Parse each coordinate string to DoubleWritable and add to the list of cords
                for(int j = 0; j < dim; j++){
                    cords.add(new DoubleWritable(Double.parseDouble(cordsString[j])));
                }

                // Create a new centroid with the cords and index
                Centroid centroid = new Centroid(cords, new IntWritable(index), new IntWritable(0)); // new IntWritable(0) the number of points assigned to centroids

                // Append the centroid to the writer
                writer.append(new IntWritable(index), centroid);
                reader.close();
            }
        }
        catch(IOException ex){
            System.out.println("Error reading file");
            ex.printStackTrace();
        }

        // Sync and close the writer
        writer.syncFs();
        writer.close();
    }

    public static void main( String[] args )
    {
        if(args.length != 6){
            System.out.println("Usage: KMeans.jar < k d n input output threshold >");
        }

        try {
            final Configuration conf = new Configuration();

            int k = Integer.parseInt(args[0]);
            conf.setInt("k", k);
            int dim = Integer.parseInt(args[1]);
            conf.setInt("dim", dim);

            Path inputPath = new Path(args[3]);
            Path outputPath = new Path(args[4]);
            Path centroidsPath = new Path("centroids/centroids.seq");

            // Generate random centroids
            generateRandomCentroids(conf, k, dim, Integer.parseInt(args[2]), centroidsPath, args[3]);

            // Initialize variables and configurations
            int iteration = 0;
            conf.set("centroidsPath", centroidsPath.toString());
            conf.set("threshold", args[5]);
            Job job = null;
            boolean converged = false;
            FileSystem hdfs = FileSystem.get(conf);

            // Run K-means iterations until convergence
            while (!converged) {
                // Create a new MapReduce job
                job = new Job(conf, "kmeans");

                // Set input and output paths
                FileInputFormat.addInputPath(job, inputPath);
                FileOutputFormat.setOutputPath(job, outputPath);
                // Set main class JAR file
                job.setJarByClass(KMeans.class);
                // Set mapper, combiner, and reducer classes
                job.setMapperClass(KMeansMapper.class);
                job.setCombinerClass(KMeansCombiner.class);
                job.setReducerClass(KMeansReducer.class);
                // Set key-value classes for output
                job.setOutputKeyClass(IntWritable.class); //Output of Job
                job.setOutputValueClass(Centroid.class);
                job.setMapOutputKeyClass(Centroid.class); //Output of Mapper
                job.setMapOutputValueClass(Point.class);
                job.setNumReduceTasks(1); // Set the number of reduce task, job will have only 1 Reduce Task

                // Execute the job and wait for completion
                boolean done = job.waitForCompletion(true);

                // Check if the job done successfully
                if (!done) {
                    System.exit(0);
                }
                // Check if the algorithm has converged
                converged = (job.getCounters().findCounter(KMeansReducer.COUNTER.CONVERGED).getValue() == 1); //Counter is Enum inside KMeans Reducer
                // Increment iteration counter
                iteration++;
                // Delete the output folder if it exists and the algorithm has not converged
                if (hdfs.exists(outputPath) && !converged) {
                    hdfs.delete(outputPath, true);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
