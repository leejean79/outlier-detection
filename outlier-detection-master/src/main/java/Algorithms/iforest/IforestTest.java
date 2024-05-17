package Algorithms.iforest;


import java.util.ArrayList;
import java.util.List;

public class IforestTest {

    public static void main(String[] args) {

        List<double[]> data = new ArrayList<>();

        double[] array1 = {1, 2};
        double[] array2 = {1.1, 2};
        double[] array3 = {1, 2.1};
        double[] array4 = {1.1, 2.1};
        double[] array5 = {0.1, 0.1};

        data.add(array1);
        data.add(array2);
        data.add(array3);
        data.add(array4);
        data.add(array5);

        IForest iForest = new IForest(3, 3);

        iForest.fit(data);

        double score = iForest.anomalyScore(array1, 5);

        System.out.println(score);



    }

}
