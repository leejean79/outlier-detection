package Algorithms.iforest.test;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static Algorithms.iforest.utlis.Common.zipWithIndex;

public class Test {
    public static void main(String[] args) {
        List<Integer> numbers = new ArrayList<>();
        numbers.add(1);
        numbers.add(2);
        numbers.add(3);

        Tuple2[] index = zipWithIndex(numbers);

        // 打印输出配对结果
        for (Object pair : index) {
            System.out.println(pair);

        }
    }
}
