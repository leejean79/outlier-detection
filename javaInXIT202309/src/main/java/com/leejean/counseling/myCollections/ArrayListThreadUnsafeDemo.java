package com.leejean.counseling.myCollections;

import java.util.ArrayList;
import java.util.List;

public class ArrayListThreadUnsafeDemo {

    public static void main(String[] args) {
        List<Integer> list = new ArrayList<>();

        // 创建两个线程，同时对 ArrayList 进行添加操作
        Thread thread1 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                list.add(i);
            }
        });

        Thread thread2 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                list.add(i);
            }
        });

        // 启动两个线程
        thread1.start();
        thread2.start();

        // 等待两个线程执行完毕
        try {
            thread1.join();
            thread2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 输出 ArrayList 的大小
        System.out.println("ArrayList size: " + list.size());
    }
}