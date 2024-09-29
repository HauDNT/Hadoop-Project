package com.mycompany.multiply.matrices;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class HadoopMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] elements = value.toString().split(",\\s*");

        String matrixName = elements[0];

        int row = Integer.parseInt(elements[1]);
        int col = Integer.parseInt(elements[2]);
        int elementValue = Integer.parseInt(elements[3]);

        if (matrixName.equals("A")) {
            for (int k = 0; k < col; k++) {
                context.write(new Text(col + "," + k), new Text("A," + row + "," + elementValue));
            }
        } else {
            for (int i = 0; i < row; i++) {
                context.write(new Text(i + "," + row), new Text("B," + col + "," + elementValue));
            }
        }
    }
}
