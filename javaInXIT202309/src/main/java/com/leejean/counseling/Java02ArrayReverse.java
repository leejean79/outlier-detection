package com.leejean.counseling;

//int a[]= {29,83,47,29,87,48,237,49,237,489,10};
//逆序存放该数组

import java.util.Arrays;

public class Java02ArrayReverse {

    public static void main(String[] args) {

        int a[]= {29,83,47,29,87,48,237,49,237,489,10};

//        创建一个中间变量，用来存储临时数据
        int t;
//        将数组一分为二，从第一个元素开始遍历，直到数组中间位置
        for (int i = 0; i < a.length/2; i++) {
            //            将第一个数存入临时变量，
            t = a[i];
//            将最后一个数赋值给第一个数
            a[i] = a[a.length - (i+1)];
//            再将临时变量的值赋值给最后一个
            a[a.length-1 -i] = t;

        }

        System.out.println(Arrays.toString(a));

    }
}
