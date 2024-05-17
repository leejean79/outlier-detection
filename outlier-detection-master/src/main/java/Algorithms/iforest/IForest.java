package Algorithms.iforest;
import java.util.ArrayList;
import java.util.List;

public class IForest {

    private int numTrees; // 孤立树的数量
    private int maxTreeDepth; // 孤立树的最大深度
    private List<Node> forest; // 存储孤立树的列表

    public IForest(int numTrees, int maxTreeDepth) {
        this.numTrees = numTrees;
        this.maxTreeDepth = maxTreeDepth;
        this.forest = new ArrayList<>();
    }

    public void fit(List<double[]> X) {
        int numInstances = X.size();
        int numFeatures = X.get(0).length;

//        一个构建numTrees颗树
        for (int i = 0; i < numTrees; i++) {
//            每一颗树一组特征值
            List<double[]> sampledX = new ArrayList<>();
            for (int j = 0; j < numInstances; j++) {
//                随机采样numInstances个样本
                int randomIndex = (int) (Math.random() * numInstances);
                sampledX.add(X.get(randomIndex));
            }

            Node tree = buildTree(sampledX, maxTreeDepth);
            forest.add(tree);
        }
    }

    /**
     *
     * @param X
     * @param currentDepth 树深
     * @return
     */
    private Node buildTree(List<double[]> X, int currentDepth) {
        if (currentDepth <= 0 || X.size() <= 1) {
            return new Node(X.size());
        }

        int numFeatures = X.get(0).length;
//        随机选择某一属性
        int randomFeatureIndex = (int) (Math.random() * numFeatures);

        double[] featureColumn = new double[X.size()];
//        取出subSample中某属性的所有值
        for (int i = 0; i < X.size(); i++) {
            featureColumn[i] = X.get(i)[randomFeatureIndex];
        }

        double minVal = Double.POSITIVE_INFINITY;
        double maxVal = Double.NEGATIVE_INFINITY;
        for (double val : featureColumn) {
            if (val < minVal) {
                minVal = val;
            }
            if (val > maxVal) {
                maxVal = val;
            }
        }

        double randomSplitValue = minVal + Math.random() * (maxVal - minVal);
        List<double[]> leftX = new ArrayList<>();
        List<double[]> rightX = new ArrayList<>();

        for (double[] instance : X) {
            if (instance[randomFeatureIndex] < randomSplitValue) {
                leftX.add(instance);
            } else {
                rightX.add(instance);
            }
        }

        Node leftNode = buildTree(leftX, currentDepth - 1);
        Node rightNode = buildTree(rightX, currentDepth - 1);

        return new Node(randomFeatureIndex, randomSplitValue, leftNode, rightNode);
    }

    /**
     *
     * @param instance
     * @param numInstances subSample size
     * @return
     */
    public double anomalyScore(double[] instance, int numInstances) {
        double pathLengthSum = 0.0;

        for (Node tree : forest) {
            pathLengthSum += getPathLength(instance, tree, 0);
        }

        double averagePathLength = pathLengthSum / numTrees;
        return Math.pow(2, -averagePathLength / Node.c(numInstances));
    }

    private double getPathLength(double[] instance, Node node, int currentDepth) {
        if (node.isLeafNode()) {
            return currentDepth + Node.c(node.getNumInstances());
        }

        if (instance[node.getSplitFeatureIndex()] < node.getSplitValue()) {
            return getPathLength(instance, node.getLeftNode(), currentDepth + 1);
        } else {
            return getPathLength(instance, node.getRightNode(), currentDepth + 1);
        }
    }


}
