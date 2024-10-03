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
        int numColsB = context.getConfiguration().getInt("numColsB", -1);

        switch (matrixName) {
            case "A":
                // Ghi dữ liệu của ma trận A
                for (int k = 0; k < numColsB; k++) { // Chạy qua số cột của ma trận B
                    context.write(new Text(row + "," + k), new Text("A," + col + "," + elementValue));
                }   break;
            case "B":
                // Ghi dữ liệu của ma trận B
                for (int i = 0; i < numColsB; i++) { // Chạy qua số hàng của ma trận A
                    context.write(new Text(i + "," + col), new Text("B," + row + "," + elementValue));
                }   break;
            default:
                context.write(new Text("NaN, NaN"), new Text("NaN, NaN, NaN"));
                break;
        }
    }
}
