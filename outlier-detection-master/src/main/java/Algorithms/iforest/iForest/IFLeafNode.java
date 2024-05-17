package Algorithms.iforest.iForest;

public class IFLeafNode extends IFNode{

    private long numInstance;

    public IFLeafNode(long numInstance) {
        this.numInstance = numInstance;
    }

    public IFLeafNode() {
    }

    public long getNumInstance() {
        return numInstance;
    }
}
