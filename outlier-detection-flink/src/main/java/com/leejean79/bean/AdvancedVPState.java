package com.leejean79.bean;

import com.leejean79.mtree.MTree;

import java.util.HashMap;

public class AdvancedVPState {

    public MTree<Data> tree;
    //    data 的 id 和 data
    public HashMap<Integer, Data> hashMap;

    public AdvancedVPState(MTree<Data> mTree, HashMap<Integer, Data> hashMap) {
        this.tree = mTree;
        this.hashMap = hashMap;
    }

    public MTree<Data> getTree() {
        return tree;
    }

    public void setTree(MTree<Data> tree) {
        this.tree = tree;
    }

    public HashMap<Integer, Data> getHashMap() {
        return hashMap;
    }

    public void setHashMap(HashMap<Integer, Data> hashMap) {
        this.hashMap = hashMap;
    }


}
