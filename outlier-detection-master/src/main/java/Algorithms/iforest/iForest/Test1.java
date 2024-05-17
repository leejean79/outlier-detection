package Algorithms.iforest.iForest;

import Algorithms.iforest.evaluation.BinaryClassificationMetrics;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static Algorithms.iforest.utlis.PreprocessUtil.readData;

public class Test1 {

    public static void main(String[] args) throws Exception {

        //read data file
        String csvFile = "src/main/java/data/breastw.csv";
        String csvSplitBy = ",";
        List<double[]> data = readData(csvFile, csvSplitBy);

//        for (double[] datum : data) {
//            for (int i = 0; i < datum.length; i++) {
//                System.out.println(datum[i]);
//            }
//
//        }

        IForest iForest = new IForest(100, 256, 0.7f, 100, false);
//
        long seed = iForest.getSeed();
        Random random = new Random(seed);
//      return (treeId, points4build)
        List<Tuple2<Integer, List<double[]>>> splitData = iForest.splitData(data);
//        for (int i = 0; i < splitData.size(); i++) {
//            Tuple2<Integer, List<double[]>> tuple2 = splitData.get(i);
//            tuple2.f1.forEach(arr -> {
//                for (int j = 0; j < arr.length; j++) {
//                    System.out.println(tuple2.f0 + ": " + arr[j]);
//                }
//            });
//        }
//
        List<IFNode> trees = new ArrayList<>();
        splitData.forEach(dataTuple2 -> {
            // get each sample for building a tree
            List<double[]> points = dataTuple2.f1;
            int treeId = dataTuple2.f0;
            // remove 1st id col and the last col label
            ArrayList<double[]> x = new ArrayList<>();
            points.forEach(point -> {
                double[] arr = new double[points.get(0).length - 2];
                System.arraycopy(point, 1, arr, 0, point.length - 2);
                x.add(arr);
            });
//            x.forEach(point -> {
//                for (int i = 0; i < point.length; i++) {
//                    System.out.println(point[i]);
//                }
//                System.out.println();
//                System.out.println();
//            });
            IFNode aTree = iForest.buildTree(x, treeId);
            trees.add(aTree);
        });
//
//
        IForestModel iForestModel = new IForestModel(trees);
        iForestModel.setContamination(0.35);
//        iForestModel.setApproxQuantileRelativeError();
//
        double[][] predicts = iForestModel.predict(data, iForest.getMaxSamples());
//
        for (int i = 0; i < predicts.length; i++) {
            double[] results = predicts[i];
            double score = results[results.length - 2];
            double predict = results[results.length - 1];
            System.out.println("id: " + results[0] + "score: " + score + " predict: " + predict);
        }
//
        // get the predict score column
        double[] scores = iForestModel.selectColumn(predicts[0].length - 2, predicts);
        // get the original label from source data
        double[] lables = new double[data.size()];
        for (int i = 0; i < data.size(); i++) {
            int yNum = data.get(i).length - 1;
            lables[i] = data.get(i)[yNum];
            if (lables[i] == 2){
                lables[i] = 0;
            }else {
                lables[i] = 1;
            }
        }

        double[] predict = iForestModel.selectColumn(predicts[0].length - 1, predicts);

        for (int i = 0; i < predicts.length; i++) {
            int l = predicts[0].length;
            System.out.println("id: " + predicts[i][0] + " score: " + predicts[i][l-2] + " predict: " +predicts[i][l-1] + " label: " +lables[i] );

        }



        BinaryClassificationMetrics binaryMetrics = new BinaryClassificationMetrics(predict, lables);

        double recall = binaryMetrics.calculateRecall();

        System.out.println(recall);

        System.out.println();




    }

}
