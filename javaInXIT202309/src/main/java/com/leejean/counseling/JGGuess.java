package com.leejean.counseling;

import java.util.Scanner;

public class JGGuess {

    public static void jg(int n){
        while(n!=1){
            if (n%2==0) System.out.print(n=n/2);
            else System.out.print(n=n*3+1);
            System.out.print(" ");
        }
    }

    public static void main(String[] args) {
        Scanner sc=new Scanner(System.in);
        int n=sc.nextInt();
        jg(n);
    }
}
