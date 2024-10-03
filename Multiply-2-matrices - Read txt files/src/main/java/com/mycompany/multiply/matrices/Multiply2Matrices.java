package com.mycompany.multiply.matrices;

import java.io.IOException;
import com.mycompany.multiply.matrices.HadoopMapper;

public class Multiply2Matrices {

    public static void main(String[] args) throws Exception {
        HadoopMapper mapper = new HadoopMapper();
        int[][] matrixA = mapper.readMatrixDataFromFile("/input/MatrixA.txt", "A", 2, 2);
        int[][] matrixB = mapper.readMatrixDataFromFile("/input/MatrixB.txt", "B", 2, 2);

        System.out.println("Matrix A: ");
        mapper.printMatrix(matrixA);

        System.out.println("\nMatrix B: ");
        mapper.printMatrix(matrixB);

        int[][] matrixC = new int[2][2];

        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                for (int k = 0; k < 2; k++) {
                    matrixC[i][j] += matrixA[i][k] * matrixB[k][j];
                }
            }
        }

        System.out.println("\nMatrix C: ");
        mapper.printMatrix(matrixC);
    }
}
