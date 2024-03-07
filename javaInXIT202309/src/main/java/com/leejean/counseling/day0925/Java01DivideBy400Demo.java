package com.leejean.counseling.day0925;

//代码的功能是输出400以内能被9或能被19整除的数及其和，每行输出5个数

public class Java01DivideBy400Demo {

    public static void main(String[] args) {

        int sum = 0;
//        计数器
        int n = 0;

//        遍历 1 到 400
        for (int i = 1; i < 400; i++) {
//            x 能否被 9 或 19 整除
            if (i % 9 == 0|| i %19 == 0){
                sum += i;
                System.out.printf("%6d", i);
                n ++;
                if (n % 5==0){
                    System.out.println();
                }
            }
//          如果能被整除，x 加起来
        }
//
//        输出：
//          每行输出 5 个

        System.out.printf("\nsum=%d\n", sum);


    }

}
