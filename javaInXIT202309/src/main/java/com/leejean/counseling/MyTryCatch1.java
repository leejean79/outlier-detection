package com.leejean.counseling;

import java.util.Scanner;

public class MyTryCatch1 {

    public static int divide(int a,int b) throws Exception{
        return a/b;
    }

    public static void main(String[] args) throws Exception {

        Scanner scanner = new Scanner(System.in);

        int a=5;
        int b= Integer.parseInt(scanner.nextLine());
        try{
            int c = a/b;
        }catch(Exception e){

            System.out.println(e.getMessage());
        }

        System.out.println("后续代码");

//        divide(a, b);

    }
}
