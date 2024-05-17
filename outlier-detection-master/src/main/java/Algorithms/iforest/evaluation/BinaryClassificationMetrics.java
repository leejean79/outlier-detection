package Algorithms.iforest.evaluation;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

public class BinaryClassificationMetrics {

    double[] predict;
    double[] label;


    public BinaryClassificationMetrics(double[] predicts, double[] labels) {

        predict = new double[predicts.length];
        label = new double[labels.length];
        System.arraycopy(predicts, 0, predict, 0, predicts.length);
        System.arraycopy(labels, 0, label, 0, labels.length);

    }

    /**
     * Computes the area under the receiver operating characteristic (ROC) curve.
     */
    public double areaUnderROC(){

        double auc = EvaluationMethods.auc(predict, label);

        return auc;
    }

    public double calculateRecall(){

        double recall = EvaluationMethods.recall(label, predict);

        return recall;

    }



}
