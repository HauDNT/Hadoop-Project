package com.mycompany.multiply.matrices;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int[] A = new int[2];
        int[] B = new int[2];
        
        for (Text val : values) {
            String[] elements = val.toString().split(",");
            if (elements[0].equals("A")) {
                A[Integer.parseInt(elements[2])] = Integer.parseInt(elements[1]);
            } else {
                B[Integer.parseInt(elements[2])] = Integer.parseInt(elements[1]);
            }
        }
        
        int result = A[0] * B[0] + A[1] * B[1];
        context.write(new Text(key.toString()), new Text(String.valueOf(result)));   
    }
}
