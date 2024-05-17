package Algorithms.iforest.iForest;

import Algorithms.iforest.utlis.MyIdentifiable;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IForestModel {

    private String uid;
    private List<IFNode> _trees;
    double EulerConstant = 0.5772156649;
    private  double threshold = -1;
    /**
     * contamination: The proportion of outliers in the data set (0< contamination < 1).
     * * It will be used in the prediction. In order to enhance performance,
     * Our method to get anomaly score threshold adopts DataFrameStsFunctions.approxQuantile,
     * which is designed for performance with some extent accuracy loss.
     * Set the param approxQuantileRelativeError (0 < e < 1) to calculate
     * an approximate quantile threshold of anomaly scores for large dataset.
     */
    private double contamination = 0.1;
    // Relative Error for Approximate Quantile (0 <= value <= 1),  default is 0.
    private double approxQuantileRelativeError = 0;

    public IForestModel(List<IFNode> _trees) {
        this.uid = MyIdentifiable.generateForestId("IForest");
        this._trees = _trees;
    }

    public double getContamination() {
        return contamination;
    }

    public void setContamination(double contamination) {
        this.contamination = contamination;
    }

    public double getApproxQuantileRelativeError() {
        return approxQuantileRelativeError;
    }

    public void setApproxQuantileRelativeError(double approxQuantileRelativeError) {
        this.approxQuantileRelativeError = approxQuantileRelativeError;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    /**
     * A function to calculate an expected path length with a specific data size.
     * @param size Data size.
     * @return An expected path length.
     */
    public double avgLength(double size) {
        if (size > 2){
            double H = Math.log(size - 1) + EulerConstant;
            return 2*H - 2*(size - 1) / size;
        }else if (size == 2){
            return 1.0;
        }else {
            return 0.0;
        }

    }

    /**
     * Calculate an average path length for a given feature set in a forest.
     * @param features A Vector stores feature values.
     * @return Average path length.
     */
    public double calAvgPathLength(double[] features){
        double totalLength = 0.0;
        double avgPathLength = 0.0;
        for (int i = 0; i < _trees.size(); i++) {
            IFNode ifNode = _trees.get(i);
            totalLength += calPathLength(features, ifNode, 0);
        }
        avgPathLength = totalLength / _trees.size();
        return avgPathLength;
    }

    /**
     * Calculate a path langth for a given feature set in a tree.
     * @param features A array stores feature values.
     * @param ifNode Tree's root node.
     * @param currentPathLength Current path length.
     * @return Path length in this tree.
     */
    private double calPathLength(double[] features, IFNode ifNode, int currentPathLength) {
        if (ifNode instanceof IFLeafNode){
            return currentPathLength + avgLength(((IFLeafNode) ifNode).getNumInstance());
        }else if (ifNode instanceof IFInteralNode){
            int attrIndex = ((IFInteralNode) ifNode).getFeatureIndex();
            if (features[attrIndex] < ((IFInteralNode) ifNode).getFeatureValue()){
                return calPathLength(features, ((IFInteralNode) ifNode).getLeftChild(), currentPathLength+1);
            }else {
                return calPathLength(features, ((IFInteralNode) ifNode).getRightChild(), currentPathLength+1);

            }
        }else {
            return 0.0;
        }

    }

    public double[][] predict(List<double[]> data, double maxSamples){

        int numSamples = data.size();

        double possibleMaxSamples = 0.0;

        if (maxSamples > 1.0 ){
            possibleMaxSamples = maxSamples;
        }else{
            possibleMaxSamples = maxSamples * numSamples;
        }

        // calculate anomaly score
        double normFactor = avgLength(possibleMaxSamples);

        // define a matrix to keep the result of prediction
        // which is: [[id, feature1, feature1, ..., label, score, predict]]
        int columes = data.get(0).length + 2;
        double[][] scoreDataset = new double[numSamples][columes];
        for (int i = 0; i < numSamples; i++) {
            for (int j = 0; j < data.get(i).length; j++) {
                scoreDataset[i][j] = data.get(i)[j];
            }
        }
        // add columns of score and label
        for (int i = 0; i < numSamples; i++) {
            double[] features = data.get(i);
            // exclude the columns: id and label
            double[] feats = new double[features.length - 2];
            System.arraycopy(features, 1, feats, 0, features.length-2);
            double avgPathLength = calAvgPathLength(feats);
            double score = Math.pow(2, -avgPathLength / normFactor);
            scoreDataset[i][columes - 2] = score;
            scoreDataset[i][columes - 1] = 0;
        }

        // the colume number of score
        int scoreColNum = scoreDataset[0].length - 2;
        System.out.println("scoreNum: " + scoreColNum);
        // take out the score column
        double[] scoreCol = selectColumn(scoreColNum, scoreDataset);


        if (threshold < 0){

            System.out.println("threshold is not set, calculating the anomaly threshold according to param contamination..");
            // calculate the threshold
            threshold = approxQuantile(scoreCol, contamination, approxQuantileRelativeError);
            System.out.println("threshold: " + threshold);
        }

        // set anomaly instance label 1
        for (int i = 0; i < scoreDataset.length; i++) {
            if (scoreDataset[i][columes -2] > threshold){
                scoreDataset[i][columes -1] = 1;
            }else {
                scoreDataset[i][columes -1] = 0;
            }
        }

        return scoreDataset;


    }

    private double approxQuantile(double[] scoreCol, double contamination, double approxQuantileRelativeError) {

        Arrays.sort(scoreCol);
        int index = (int) Math.ceil((1 - contamination) * scoreCol.length);
        double adjustment = Math.ceil(index * approxQuantileRelativeError);
        index += adjustment;
        double quantile = scoreCol[index];
        return quantile;
    }

    /**
     *
     * @param scoreColNum the number of score column
     * @param scoreDataset the result dataset
     * @return the column of score
     */
    public double[] selectColumn(int scoreColNum, double[][] scoreDataset) {

        double[] scoreCol = new double[scoreDataset.length];
        for (int i = 0; i < scoreDataset.length; i++) {
            scoreCol[i] = scoreDataset[i][scoreColNum];
        }

        return scoreCol;
    }

    class IForestSummary implements Serializable{

        double[][] predictions;
        int scoreCol;
        int predictCol;

        public IForestSummary(double[][] predictions, int scoreCol, int predictCol) {
            this.predictions = predictions;
            this.scoreCol = scoreCol;
            this.predictCol = predictCol;
        }

        double[] predict = selectColumn(predictCol, predictions);

        public long numAnomalies(){
            long anomalies = 0;

            for (int i = 0; i < predict.length; i++) {
                if (predict[i] > 0){
                    anomalies ++;
                }
            }

        return anomalies;
        }
    }


}


