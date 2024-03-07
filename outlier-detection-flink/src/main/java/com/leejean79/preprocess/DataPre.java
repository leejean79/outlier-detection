package com.leejean79.preprocess;

import java.io.*;

public class DataPre {

    public static void main(String[] args) {

        File file = new File("/Users/lijing/Documents/IdeaProjects/outlier-detection-flink/src/main/java/com/leejean79/outlierDetect/power_data.txt"); // 替换为实际的文件路径

        String csvFilePath = "power_data2.csv";


        try (BufferedReader reader = new BufferedReader(new FileReader(file));
             BufferedWriter writer = new BufferedWriter(new FileWriter(csvFilePath))) {

            String line;
            int i = 1;
            while ((line = reader.readLine()) != null) {
                line = i + "&" + line;
                writer.write(line);
                writer.newLine();
                i ++;
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
