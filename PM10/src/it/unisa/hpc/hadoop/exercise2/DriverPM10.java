/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unisa.hpc.hadoop.exercise2;

import it.unisa.hpc.hadoop.exercise2.DriverPM10;
import it.unisa.hpc.hadoop.exercise2.MapperPM10;
import it.unisa.hpc.hadoop.exercise2.ReducerPM10;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author lucia
 */
public class DriverPM10 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
       
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(DriverPM10.class);
    job.setMapperClass(MapperPM10.class);
    job.setCombinerClass(ReducerPM10.class);
    job.setReducerClass(ReducerPM10.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    //job.setInputFormatClass(cls);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
    
}
