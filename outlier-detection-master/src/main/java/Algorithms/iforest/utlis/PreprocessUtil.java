package Algorithms.iforest.utlis;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PreprocessUtil {

    public static List<double[]> readData(String filePath, String splitBy) throws Exception {

        String line;
        List<double[]> data = new ArrayList<>();
        //data preprocess
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            while ((line = br.readLine()) != null) {
                String[] row = line.split(splitBy);
                double[] newArr = new double[row.length];
                for (int i = 0; i < row.length; i++) {
                    newArr[i] = Double.parseDouble(row[i]);
                }
                data.add(newArr);
//                System.arraycopy(row, 0, newArr, 0, newArr.length);
//                double[] doubles = new double[newArr.length];
//                for (int i = 1; i < newArr.length; i++) {
//                    doubles[i] = Double.parseDouble(newArr[i]);
//                    System.out.println(doubles[i]);
                }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return data;

    }
}
