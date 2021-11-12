/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unisa.hpc.hadoop.exercise2;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author lucia
 */
public class MapperPM10 extends Mapper<Object, Text, Text, IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private int threashold = 50;

    public void map(Object key, Text value, Mapper.Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
          String line = itr.nextToken();
          String[ ] firstDivision = line.split(",");
           word.set((firstDivision[0]));
           String[ ] secondDivision = firstDivision[1].split("\t");
           if(Float.parseFloat(secondDivision[1]) > this.threashold){
               
               context.write(word, one);
           }
           
      }
    }
}