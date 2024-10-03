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
            int value = Integer.parseInt(elements[3]);

            if (matrixName.equals("A")) {
                A_values.add(value);
            } else {
                B_values.add(value);
            }
        }

        // Nhân các giá trị từ A và B
        if (!A_values.isEmpty() && !B_values.isEmpty()) {
            for (int a : A_values) {
                for (int b : B_values) {
                    context.write(new Text(key.toString()), new IntWritable(a * b));
                }
            }
        } else {
            // Nếu không có giá trị nào, có thể ghi một thông báo hoặc không ghi gì cả
            context.write(new Text(key.toString()), new IntWritable(0)); // Hoặc không ghi gì
        }
    }
}
