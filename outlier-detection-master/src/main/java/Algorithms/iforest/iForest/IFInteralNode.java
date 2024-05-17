package Algorithms.iforest.iForest;


public class IFInteralNode extends IFNode {

    private IFNode leftChild;
    private IFNode rightChild;
    private  int featureIndex;
    private double featureValue;

    public IFInteralNode(IFNode leftChild, IFNode rightChild, int featureIndex, double featureValue) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.featureIndex = featureIndex;
        this.featureValue = featureValue;
    }

    public IFInteralNode() {

    }

    public IFNode getLeftChild() {
        return leftChild;
    }

    public IFNode getRightChild() {
        return rightChild;
    }

    public int getFeatureIndex() {
        return featureIndex;
    }

    public double getFeatureValue() {
        return featureValue;
    }
}
