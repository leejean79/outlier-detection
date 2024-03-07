package com.leejean.counseling;

//代码的功能是输出400以内能被9或能被19整除的数及其和，每行输出5个数

import java.util.Arrays;

public class Java01DivideBy400Demo {

    public static void main(String[] args) {

        int sum = 0;
        int elementsPerLine = 5;
        int n = 0;

//        遍历 400：
        for (int i = 0; i < 400; i++) {

            if (i%9 == 0 || i%19 == 0){
                sum += i;
                System.out.printf("%6d", i);
                n++;
                if (n%5 == 0 ){
                    System.out.println();
                }
            }

        }

        System.out.printf("\nsum=%d\n", sum);
//            任取 x，判断能否被 9 或 19 整除
//                if true：
//                    输出 x，并求和
//                      每行输出5个数


    }
}
