package com.mycompany.multiply.matrices;

import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Integer> A_values = new ArrayList<>();
        List<Integer> B_values = new ArrayList<>();

        for (Text val : values) {
            String[] elements = val.toString().split(",");
            String matrixName = elements[0];
            int index = Integer.parseInt(elements[1]);
            int value = Integer.parseInt(elements[2]);

            if (matrixName.equals("A")) {
                A_values.add(value);
            } else {
                B_values.add(value);
            }
        }

        // Nhân các giá trị từ A và B
        for (int a : A_values) {
            for (int b : B_values) {
                context.write(new Text(key.toString()), new IntWritable(a * b));
            }
        }
    }
}
