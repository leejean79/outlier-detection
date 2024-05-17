package Algorithms.iforest.evaluation;

import org.apache.commons.math3.stat.ranking.NaNStrategy;
import org.apache.commons.math3.stat.ranking.NaturalRanking;
import org.apache.flink.api.java.tuple.Tuple2;


public class EvaluationMethods {

    public static double auc(double[] labels, double[] predicts){

        // 使用自然排序来计算排名
        NaturalRanking ranking = new NaturalRanking(NaNStrategy.MINIMAL, NaturalRanking.DEFAULT_TIES_STRATEGY);
        double[] ranks = ranking.rank(predicts);

        // 计算正样本的秩和
        double sumRanksPositive = 0;
        for (int i = 0; i < labels.length; i++) {
            if (labels[i] == 1) {
                sumRanksPositive += ranks[i];
            }
        }

        // 计算 AUC 值
        double auc = (sumRanksPositive - (labels.length * (labels.length + 1) / 2)) / (labels.length - sumRanksPositive);

        return auc;
    }

    public static double recall(double[] labels, double[] predicts){

        Tuple2 tpFn = tpFn(labels, predicts);
        int tp = (int) tpFn.f0;
        int fn = (int) tpFn.f1;

        return (double) tp / (tp + fn);
    }


    private  static Tuple2 tpFn(double[] labels, double[] predicts) {
        // 计算 TP 和 FN
        int truePositives = 0;   // 真正例数
        int falseNegatives = 0;  // 误负例数

        for (int i = 0; i < labels.length; i++) {
            if (labels[i] == 1 && predicts[i] == 1) {
                truePositives++;
            } else if (labels[i] == 1 && predicts[i] == 0) {
                falseNegatives++;
            }
        }

        return new Tuple2<Integer, Integer>(truePositives, falseNegatives);

    }

}
