package Algorithms.iforest.iForest;

import Algorithms.iforest.utlis.Common;
import Algorithms.iforest.utlis.RangeIterator;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.random.RandomGeneratorFactory;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;
import java.util.function.Function;

public class IForest {

    // numbers of trees for build
    private int numTrees = 100;
    //The number of samples to draw from data to train each tree
    private double maxSamples = 1.0;
    // The number of features to draw from data to train each tree
    private float maxFeatures = 1.0f;
    private int maxDepth = 10;
    private boolean bootstrap = false;
    private long seed = this.getClass().getName().hashCode();
//    private String uid;
    private Random rng;

    public IForest(int numTrees, float maxSamples, float maxFeatures, int maxDepth, boolean bootstrap) {
        this.numTrees = numTrees;
        this.maxSamples = maxSamples;
        this.maxFeatures = maxFeatures;
        this.maxDepth = maxDepth;
        this.bootstrap = bootstrap;
//        this.uid = prefix+"_"+ UUID.randomUUID().toString();
        this.rng = new Random(seed);
    }

    public IForest() {
    }

    public int getNumTrees() {
        return numTrees;
    }

    public void setNumTrees(int numTrees) {
        this.numTrees = numTrees;
    }

    public double getMaxSamples() {
        return maxSamples;
    }

    public void setMaxSamples(float maxSamples) {
        this.maxSamples = maxSamples;
    }

    public float getMaxFeatures() {
        return maxFeatures;
    }

    public void setMaxFeatures(float maxFeatures) {
        this.maxFeatures = maxFeatures;
    }

    public int getMaxDepth() {
        return maxDepth;
    }

    public void setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
    }

    public boolean isBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(boolean bootstrap) {
        this.bootstrap = bootstrap;
    }

    public long getSeed() {
        return seed;
    }

    public void setSeed(long seed) {
        this.seed = seed;
    }

//    public String getUid() {
//        return uid;
//    }
//
//    public void setUid(String uid) {
//        this.uid = uid;
//    }



    long numSamples = 0l;
    int possibleMaxSamples = 0;

    /**
     * Sample and split data to $numTrees groups, each group will build a tree.
     * @param data Training Dataset
     * @return A Tupple[], where key is the tree index, value is an array of data instances for training a iTree.
     */
    public List<Tuple2<Integer, List<double[]>>> splitData(List<double[]> data){

        double fraction = 0.0;

        numSamples = data.size();
        System.out.println(numSamples);
        if (maxSamples > 1){
            fraction = maxSamples / numSamples;
        }else {
            fraction = maxSamples;
        }

        possibleMaxSamples = (int)(fraction * numSamples);
        System.out.println(possibleMaxSamples);

        // use advanced apache common math3 random generator
        RandomDataGenerator advancedRgn = new RandomDataGenerator(RandomGeneratorFactory.createRandomGenerator(new Random(rng.nextLong())));

//        RangeIterator longIterator = new RangeIterator(0, numSamples);

        // SampledIndices is a two-dimensional array, that generates sampling row indices in each iTree.
        // E.g. [[1, 3, 6, 4], [6, 4, 2, 5]] indicates that the first tree has
        // data consists of the 1, 3, 6, 4 row samples, the second tree has data
        // consists of the 6, 4, 2, 5 row samples.
        // Note: sampleIndices will occupy about maxSamples * numTrees * 8
        // byte memory in the driver.
        long[][] sampleIndices = new long[numTrees][];
        for (int i = 0; i < numTrees; i++) {
            RangeIterator longIterator = new RangeIterator(0, numSamples);
            long[] indices = reservoirSampleAndCount(longIterator, possibleMaxSamples, rng.nextLong()).f0;
            sampleIndices[i] = indices;
        }

        // [(treeId: Int, indices: Array[Long]),....]
        // [(1, [1, 3, 6, 4]), (2, [6, 4, 2, 5]), ..., ]
        Tuple2[] splitedDataIndex = new Tuple2[sampleIndices.length];
        for (int i = 0; i < sampleIndices.length; i++) {
            splitedDataIndex[i] = new Tuple2<>(i, sampleIndices[i]);
        }

        ArrayList<Tuple2<Integer, List<double[]>>> splitedData = new ArrayList<>();
        for (Tuple2 dataIndex : splitedDataIndex) {
            List<double[]> list = new ArrayList<>();
            long[] indexArr = (long[]) dataIndex.f1;
            int treeId = (int) dataIndex.f0;
            for (long index : indexArr) {
                if (index >= 0 && index < data.size()){
                    list.add(data.get((int)index));
                }
            }
            splitedData.add(new Tuple2<>(treeId, list));
        }

//      [(1,1),(3,1),(6,1),(4,1),(6,2)...]
//        List<Tuple2<Long, Integer>> flatMapped = new ArrayList<>();
//        for(Tuple2<long[], Integer> zippedTupple : zippedArr){
//            long[] indices = zippedTupple.f0;
//            int treeId = zippedTupple.f1;
//            for (int i = 0; i < indices.length; i++) {
//                long rowIndex = indices[i];
//               flatMapped.add(new Tuple2<>(rowIndex, treeId)) ;
//            }
//        }

        // group by indices
        // key is indice, value is list of the tree id
        // [(6, [1, 5, 9, treeId]),...]
//        HashMap<Long, List<Integer>> groupedMap = new HashMap<>();
//        for (Tuple2<Long, Integer> tuple : flatMapped) {
//            Long indice = tuple.f0;
//            if (!groupedMap.containsKey(indice)){
//                groupedMap.put(indice, new ArrayList<>());
//            }
//            groupedMap.get(indice).add(tuple.f1);
//        }

        // transform map to list and return the result: [(dataId, [treeId, ..]),...]
//        List<Tuple2<Long, List<Integer>>> data2Trees = new ArrayList<>();
//        ArrayList<Map.Entry<Long, List<Integer>>> list = new ArrayList<>(groupedMap.entrySet());
//        for (Map.Entry<Long, List<Integer>> entry: list) {
//            Tuple2<Long, List<Integer>> listTuple2 = new Tuple2<>(entry.getKey(), entry.getValue());
//            data2Trees.add(listTuple2);
//        }


//        for (Map.Entry<Long, List<Integer>> entry : groupedMap.entrySet()) {
//            System.out.println("indice: " + entry.getKey());
//            for (int treeId : entry.getValue()) {
//                System.out.println("  treeId: " + treeId);
//            }
//        }
//        groupedMap.entrySet().forEach(entry -> {
//            Long indice = entry.getKey();
//            List<Integer> treeIdArray = entry.getValue();
//            Stream<Tuple2> mappedStream = treeIdArray.stream().map(treeId -> new Tuple2(treeId, 1.0));
//            Map<Object, List<Tuple2>> groupedBytreeId = mappedStream.collect(Collectors.groupingBy(t -> t.f0));
//            groupedBytreeId.entrySet().forEach(tmp -> {
//                List<Tuple2> tuple2List = tmp.getValue();
//                Optional<Tuple2> reduced = tuple2List.stream().reduce((t1, t2) -> new Tuple2(tmp.getKey(), (double) t1.f1 + (double) t2.f1));
//            });
//
//            groupedBytreeId.entrySet().forEach(
//                    ent -> System.out.println(ent.getKey() + ": " + ent.getValue())
//            );
//        });



//        Map<Object, List<Object>> grouped = groupBy(flatMapped);

        return splitedData;
    }

    private <K, V> Map<K, List<V>> groupBy(Tuple2[] tuples) {
        Map<K, List<V>> groupedMap = new HashMap<>();

        for (Tuple2<K, V> tuple : tuples) {
            K key = tuple.f0;
            V value = tuple.f1;

            if (!groupedMap.containsKey(key)) {
                groupedMap.put(key, new ArrayList<>());
            }

            groupedMap.get(key).add(value);
        }

        return groupedMap;
    }


    /**
     * Reservoir sampling implementation that also returns the input size.
     * @param longIterator input size
     * @param possibleMaxSamples reservoir size
     * @param seed random seed
     * @return (samples, input size)
     */
    private  Tuple2<long[], Integer> reservoirSampleAndCount(RangeIterator longIterator, int possibleMaxSamples, long seed) {

        long[] reservoir = new long[possibleMaxSamples];
        // Put the first k elements in the reservoir.
        int i = 0;
        while (i < possibleMaxSamples && longIterator.hasNext()){
            Long item = longIterator.next();
            reservoir[i] = item;
            i ++;
        }
        // If we have consumed all the elements, return them. Otherwise do the replacement.
        if (i < possibleMaxSamples){
            // all the data has been consumed
            // If data size < k, trim the array to return only an array of data size
            long[] trimReservoir = new long[i];
            System.arraycopy(reservoir, 0, trimReservoir, 0, i);
            Tuple2<long[], Integer> sampleIndicesSize = new Tuple2<>(trimReservoir, i);
            return sampleIndicesSize;

        }else {
            // If data size > k, continue the sampling process.
            long l = i;
            Random rand = new Random(seed);
            while (longIterator.hasNext()){
                Long item = longIterator.next();
                l ++;
                // There are k elements in the reservoir, and the l-th element has been
                // consumed. It should be chosen with probability k/l. The expression
                // below is a random long chosen uniformly from [0,l)
                long replacementIndex = (long) (rand.nextDouble() * l);
                if (replacementIndex < possibleMaxSamples){
                    reservoir[(int)replacementIndex] = item;
                }

            }
            Tuple2<long[], Integer> sampleIndicesSize = new Tuple2<>(reservoir, (int)l);
            return sampleIndicesSize;
        }

    }


    // Random features select

    /**
     * Sample features to train a tree.
     * @param sampleData Input data to train a tree, each element is an instance.
     * @param maxFeatures The number of features to draw
     * @param random
     * @return Tuple (sampledFeaturesDataset, featureIdxArr),
     *         featureIdxArr is an array stores the origin feature idx before the feature sampling
     */
    public Tuple2<List<double[]>, List<Integer>> sampleFeatures(List<double[]> sampleData, double maxFeatures, Random random) {

//        define the result of sampled feature data
        List<double[]> sampledFetures = new ArrayList<>();

        // get input data feature size
        int numFeatures = sampleData.get(0).length;
        // calculate the number of sampling features
        int subFeatures;
//        given by the ratio
        if (maxFeatures < 1) {
            subFeatures = (int) (maxFeatures * numFeatures);
        } else if (maxFeatures > numFeatures) {
//            logger.warn("maxFeatures is larger than the numFeatures, using all features instead");
            subFeatures = numFeatures;
        } else {
            subFeatures = (int) maxFeatures;
        }

        if (numFeatures == subFeatures) {
            int[] arr = new int[subFeatures];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = i;
            }
            List<Integer> featureIdx = Common.intArrayToList(arr);
            return new Tuple2<>(sampleData, featureIdx);
        } else {
//            create random int featureIndex
//              create an arr in which the feature idx are follwed randomly
            int[] randomArray = Common.generateRandomUniqueArray(numFeatures);
            List<Integer> randomList = Common.intArrayToList(randomArray);
//              take the top subFeatures in randomArray, which is the featureIdx
            List<Integer> featureIdx = Common.take(randomList, subFeatures);

//          select random feature from sampleData
            List<double[]> sampledFeatures = new ArrayList<>(); // the sampled data with selected features
            Tuple2[] zippedIndex = Common.zipWithIndex(featureIdx);
            for (int i = 0; i < sampleData.size(); i++) {
                double[] point = sampleData.get(i);
                double[] sampledValues = new double[subFeatures];
//                acquire the feature values in each data
                for (int j = 0; j < zippedIndex.length; j++) {
                    Tuple2 elem = zippedIndex[j];
                    sampledValues[(int) elem.f1] = point[(int) elem.f0];
                }
                sampledFeatures.add(sampledValues);
            }
            return new Tuple2<>(sampledFeatures, featureIdx);
        }
    }

    /**
     * buildTree
     * @param data
     * @param treeId
     * @return a tree
     */
    public IFNode buildTree(List<double[]> data, int treeId){

//        for (int i = 0; i < data.size(); i++) {
//            for (int j = 0; j < data.get(0).length; j++) {
//                System.out.print(data.get(i)[j] + " ");
//            }
//            System.out.println();
//        }

        Random random = new Random(rng.nextInt() + treeId);

        Tuple2<List<double[]>, List<Integer>> sampleFeatures = sampleFeatures(data, this.maxFeatures, random);

        // build each tree and construct a forest
        List<double[]> trainingData = sampleFeatures.f0;
        List<Integer> featureIdxArr = sampleFeatures.f1;

//        System.out.println("samplefeature size: " + featureIdxArr.size());

        // calculate actual maxDepth to limit tree height
        int longestPath = (int) Math.ceil(Math.log(Math.max(2, trainingData.size())) / Math.log(2));
        int possibleMaxDepth = 0;
        if (this.maxDepth > longestPath){
            possibleMaxDepth = longestPath;
        }else {
            possibleMaxDepth = this.maxDepth;
        }

        // construct an array which stores featureIndex + 1
        int numFeatures = trainingData.get(0).length;
        // an array stores constant features index
        int[] constantFeatures = new int[numFeatures + 1];
        for (int i = 0; i < numFeatures+1; i++) {
            constantFeatures[i] = i;
        }
        // last position's value indicates constant feature offset index
        constantFeatures[numFeatures] = 0;

        // build a tree
        IFNode tree = iTree(trainingData, 0, possibleMaxDepth, constantFeatures, featureIdxArr, random);


        return tree;
    }

    public IFNode iTree(List<double[]> data, int currentPathLength, int maxDepth, int[] constantFeatures, List<Integer> featureIdxArr, Random random) {

        int constantFeatureIndex = constantFeatures[constantFeatures.length -1];
        // the condition of leaf node
        // 1. current path length exceeds max depth
        // 2. the number of data can not be splitted again
        // 3. there are no non-constant features to draw
        if (currentPathLength >= maxDepth || data.size() <= 1){
            IFLeafNode leafNode = new IFLeafNode(data.size());
            return leafNode;
        }else {
            int numFeatures = data.get(0).length;
            double attrMin = 0.0;
            double attrMax = 0.0;
            int attrIndex = -1;
            // until find a non-constant feature
            /**
             * find constant features in data's features and save them in the array of constantFeatures
             */
            boolean findConstant = true;
            while (findConstant && numFeatures != constantFeatureIndex){
                // select randomly a feature index
                int idx = random.nextInt(numFeatures - constantFeatureIndex) + constantFeatureIndex;
                attrIndex = constantFeatures[idx];
                // obtain data random feature
                double[] features = new double[data.size()];
                for (int i = 0; i < features.length; i++) {
                    features[i] = data.get(i)[attrIndex];
                }
                attrMin = Arrays.stream(features).min().getAsDouble();
                attrMax = Arrays.stream(features).max().getAsDouble();
                if (attrMin == attrMax){
                    // swap constant feature index with non-constant feature index
                    constantFeatures[idx] = constantFeatures[constantFeatureIndex];
                    constantFeatures[constantFeatureIndex] = attrIndex;
                    // constant feature index add 1, then update
                    constantFeatureIndex += 1;
                    constantFeatures[constantFeatures.length - 1] = constantFeatureIndex;
                }else{
                    findConstant = false;
                }
            }
            if (numFeatures == constantFeatureIndex){
                return new IFLeafNode(data.size());
            } else {
                // select randomly a feature value between (attrMin, attrMax)
                double attrValue = random.nextDouble() * (attrMax - attrMin) + attrMin;
                // split data according to the attrValue
                List<double[]> leftData = new ArrayList<>();
                List<double[]> rightData = new ArrayList<>();
                for (int i = 0; i < data.size(); i++) {
                    if (data.get(i)[attrIndex] < attrValue){
                        leftData.add(data.get(i));
                    }
                    else {
                        rightData.add(data.get(i));
                    }
                }
                // recursively build a tree
                return new IFInteralNode(
                        iTree(leftData, currentPathLength + 1, maxDepth, constantFeatures.clone(), featureIdxArr, random),
                        iTree(rightData, currentPathLength + 1, maxDepth, constantFeatures.clone(), featureIdxArr, random),
                        featureIdxArr.get(attrIndex),
                        attrValue
                        );
            }


        }

    }


    // transform

    // tabulate
    public  <T> T[] tabulate1(int size, Function<Integer, T> function){
        @SuppressWarnings("unchecked")
        T[] array = (T[]) new Object[size];

        for (int i = 0; i < size; i++) {
            array[i] = function.apply(i);
        }

        return array;
    }

//    public  static int[] tabulate2(int size, Function function) {
//        int[] array = new int[size];
//
//        for (int i = 0; i < size; i++) {
//            array[i] = function.apply(i);
//        }
//
//        return array;
//    }

    /**
     * the result model from dataset fit
     *
     * uid: unique ID for the Model
     * _trees: Param of trees for constructor
     */


}
