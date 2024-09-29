package com.mycompany.multiply.matrices;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Multiply2Matrices {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MatrixMultiplication <input path> <output path> <number of columns in Matrix B>");
            System.exit(-1);
        }

        // Đọc số cột của ma trận B từ tham số
        int numColsB = Integer.parseInt(args[2]);

        Configuration conf = new Configuration();
        conf.setInt("numColsB", numColsB); // Lưu số cột của Matrix B vào configuration

        Job job = Job.getInstance(conf, "Matrix Multiplication");

        job.setJarByClass(Multiply2Matrices.class);
        job.setMapperClass(HadoopMapper.class);
        job.setReducerClass(HadoopReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
