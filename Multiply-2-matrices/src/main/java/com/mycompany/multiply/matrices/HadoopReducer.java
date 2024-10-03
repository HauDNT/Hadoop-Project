package com.mycompany.multiply.matrices;

import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        List<Integer> A_values = new ArrayList<>();
//        List<Integer> B_values = new ArrayList<>();

        List<Value<Integer, Integer>> A_values = new ArrayList<>();
        List<Value<Integer, Integer>> B_values = new ArrayList<>();

        // Phân loại giá trị từ các ma trận A và B
        for (Text val : values) {
            String[] elements = val.toString().split(",");
            String matrixName = elements[0];
            int index = Integer.parseInt(elements[1]);
            int value = Integer.parseInt(elements[2]);

            if (matrixName.equals("A")) {
                A_values.add(new Value<>(index, value)); // Lưu giá trị của ma trận A
            } else if (matrixName.equals("B")) {
                B_values.add(new Value<>(index, value)); // Lưu giá trị của ma trận B
            }
        }

        // Nhân các giá trị từ A và B
        if (!A_values.isEmpty() && !B_values.isEmpty()) {
            int sum = 0; // Biến để lưu tổng tích

            // Lặp qua A_values và B_values
            for (Value<Integer, Integer> a : A_values) {
                for (Value<Integer, Integer> b : B_values) {
                    if (a.getIndex().equals(b.getIndex())) { // Nhân nếu chỉ số tương ứng
                        sum += a.getValue() * b.getValue();
                    }
                }
            }
            context.write(new Text(key.toString()), new IntWritable(sum)); // Ghi kết quả
        } else {
            context.write(new Text(key.toString()), new IntWritable(0)); // Nếu không có giá trị, ghi 0
        }
    }

    class Value<I, V> {

        private I index;
        private V value;

        public Value(I index, V value) {
            this.index = index;
            this.value = value;
        }

        public I getIndex() {
            return index;
        }

        public V getValue() {
            return value;
        }
    }
}
