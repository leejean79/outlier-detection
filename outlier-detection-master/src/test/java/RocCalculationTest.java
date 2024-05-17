import java.util.Arrays;

public class RocCalculationTest {

    public static void main(String[] args) {

        double[] actualLabels = {1, 0, 1, 0, 1, 0, 0, 1}; // 实际标签，1 表示正例，0 表示负例
        double[] predictedScores = {1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0}; // 预测得分

        // 创建一个数组来存储不同的阈值
        double[] thresholds = Arrays.stream(predictedScores).distinct().toArray();
        Arrays.sort(thresholds);

        // 计算真阳性率和假阳性率
//        double[] tpr = new double[thresholds.length];
//        double[] fpr = new double[thresholds.length];

//        for (int i = 0; i < thresholds.length; i++) {
//            double threshold = thresholds[i];
            int tp = 0; // 真阳性计数
            int fp = 0; // 假阳性计数

        double tpr = 0;
        double fpr = 0;

            for (int j = 0; j < predictedScores.length; j++) {
                if (predictedScores[j] == 1) {
                    if (actualLabels[j] == 1) {
                        tp++;
                    } else {
                        fp++;
                    }
                }


            tpr = (double) tp / Arrays.stream(actualLabels).filter(label -> label == 1).count();
            fpr = (double) fp / Arrays.stream(actualLabels).filter(label -> label == 0).count();
        }

        // 输出 ROC 曲线的数据点

            System.out.println("Threshold: "  + ", TPR: " + tpr + ", FPR: " + fpr);


    }
}
