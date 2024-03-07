package com.leejean.counseling.day0925;


//int a[]= {29,83,47,29,87,48,237,49,237,489,10};

//逆序存放该数组

import java.util.Arrays;

public class Java02ArrayReverse {

    public static void main(String[] args) {

        int a[]= {29,83,47,29,87,48,237,49,237,489,10};
//        遍历数组：
        for (int i = 0; i < a.length/2; i++) {
            int temp;
            temp = a[i];
            a[i] = a[a.length- (i+1)];
            a[a.length- (i+1)] = temp;
        }

        System.out.println(Arrays.toString(a));

//            i[0]=29,
//            临时变量 temp,
//              temp = 29,   i[0]=10, i最后一个= temp


    }


}
