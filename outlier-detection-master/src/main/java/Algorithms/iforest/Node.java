package Algorithms.iforest;

import java.io.Serializable;

public class Node implements Serializable {

    private int splitFeatureIndex;
    private double splitValue;
    private Node leftNode;
    private Node rightNode;
    private int numInstances;

    public Node(int numInstances) {
        this.numInstances = numInstances;
    }

    public Node(int splitFeatureIndex, double splitValue, Node leftNode, Node rightNode) {
        this.splitFeatureIndex = splitFeatureIndex;
        this.splitValue = splitValue;
        this.leftNode = leftNode;
        this.rightNode = rightNode;
        this.numInstances = leftNode.numInstances + rightNode.numInstances;
    }

    public int getSplitFeatureIndex() {
        return splitFeatureIndex;
    }

    public double getSplitValue() {
        return splitValue;
    }

    public Node getLeftNode() {
        return leftNode;
    }

    public Node getRightNode() {
        return rightNode;
    }

    public int getNumInstances() {
        return numInstances;
    }

    public boolean isLeafNode() {
        return leftNode == null && rightNode == null;
    }

    /**
     * A function to calculate an expected path length with a specific data size
     *
     * @param numInstances Data size.
     * @return An expected path length
     */
    public static double c(int numInstances) {
        if (numInstances <= 1) {
            return 0.0;
        }
        return 2.0 * (Math.log(numInstances - 1) + 0.5772156649) - (2.0 * (numInstances - 1) / numInstances);
    }

}
