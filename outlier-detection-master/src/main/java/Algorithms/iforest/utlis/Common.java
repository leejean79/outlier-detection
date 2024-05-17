package Algorithms.iforest.utlis;

import org.apache.flink.api.java.tuple.Tuple2;
import scala.Int;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Common {

    public static List<Integer> intArrayToList(int[] arr){

        List<Integer> list = new ArrayList<>();

        for (int num : arr) {
            list.add(num);
        }

        return list;

    }

    public static int[] generateRandomUniqueArray(int n) {
        int[] array = new int[n];

        // 初始化数组
        for (int i = 0; i < n; i++) {
            array[i] = i;
        }

        Random random = new Random();

        // 使用洗牌算法打乱数组元素顺序
        for (int i = n - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            int temp = array[i];
            array[i] = array[j];
            array[j] = temp;
        }

        return array;
    }

    public static <T> List<T> take(List<T> list, int n) {
        if (n >= list.size()) {
            return new ArrayList<>(list);
        } else {
            return new ArrayList<>(list.subList(0, n));
        }
    }

    public static int[] convertListToArray(List<Integer> list) {
        int[] array = new int[list.size()];

        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }

        return array;
    }

    public static Tuple2[] zipWithIndex(List<Integer> array) {
        Tuple2[] result = new Tuple2[array.size()];

        for (int i = 0; i < array.size(); i++) {
            Tuple2<Integer, Integer> pair = new Tuple2<>(array.get(i), i);
            result[i] = pair;
        }

        return result;
    }
}
