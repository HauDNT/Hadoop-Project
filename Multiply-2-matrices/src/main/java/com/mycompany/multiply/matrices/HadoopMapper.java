package com.mycompany.multiply.matrices;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class HadoopMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] elements = value.toString().split("\t");
        String matrixId = elements[0];
        String[] rowData = elements[1].split(",");

        for (int i = 0; i < rowData.length; i++) {
            if (matrixId.equals("A")) {
                for (int k = 0; k < rowData.length; k++) {
                    context.write(new Text(String.valueOf(i)), new Text("A," + rowData[k] + "," + k));
                }
            } else if (matrixId.equals("B")) {
                for (int k = 0; k < rowData.length; k++) {
                    context.write(new Text(String.valueOf(i)), new Text("B," + rowData[k] + "," + i));
                }
            }
        }
    }
}
