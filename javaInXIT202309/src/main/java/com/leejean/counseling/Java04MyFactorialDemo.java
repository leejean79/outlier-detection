package com.leejean.counseling;

import java.util.Scanner;

/***
 * 计算奇数列的阶乘和
 * 奇数列大小根据输入的 n 决定
 * 1!+3!+5!+...+n!，n从键盘输入
 */

public class Java04MyFactorialDemo {

    public static void main(String[] args) {

        int sum = 1;

//        Scanner 从键盘输入
        Scanner scanner = new Scanner(System.in);

        System.out.println("请输入任一个自然数 n");

        int n = scanner.nextInt();

//        计算阶乘
//            从 1 开始遍历，计算每个数的阶乘
        for (int i = 1; i < n+1; i+=2) {
            int temp = 1;
            for (int j = 1; j < i+1; j++) {
                temp = temp * j;
            }
            sum += temp;
        }

//        保存结果
//        打印
        System.out.println("sum: " + String.valueOf(sum));

    }
}
