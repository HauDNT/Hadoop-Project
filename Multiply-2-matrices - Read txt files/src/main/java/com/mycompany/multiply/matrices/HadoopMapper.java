package com.mycompany.multiply.matrices;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class HadoopMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int[][] matrixA = readMatrixDataFromFile("/input/MatrixA.txt", "A", 2, 2);
        int[][] matrixB = readMatrixDataFromFile("/input/MatrixB.txt", "B", 2, 2);
        
        printMatrix(matrixA);
        System.out.println("--------------------");
        printMatrix(matrixB);
    }

    public int[][] readMatrixDataFromFile(String filePath, String matrixType, int row, int col) {
        try {
            Configuration config = new Configuration();
            FileSystem fs = FileSystem.get(config);
            Path path = new Path(filePath);

            BufferedReader bfReader = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            int[][] matrixResult = new int[row][col];

            while ((line = bfReader.readLine()) != null) {
                String[] elements = line.split(",\\s*");
                String matrixName = elements[0];
                int matrixRow = Integer.parseInt(elements[1]);
                int matrixCol = Integer.parseInt(elements[2]);
                int matrixValue = Integer.parseInt(elements[3]);

                if (matrixName.equals(matrixType)) {
                    matrixResult[matrixRow][matrixCol] = matrixValue;
                }
            }

            bfReader.close();

            return matrixResult;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public void printMatrix(int[][] matrix) {
        for (int[] row : matrix) {
            for (int value : row) {
                System.out.print(value + " ");
            }
            System.out.println();
        }
    }
}
