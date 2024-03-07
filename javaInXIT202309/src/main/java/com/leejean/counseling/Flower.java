package com.leejean.counseling;

//编程求所有的水仙花数。所谓水仙花数是一个三位数，其每一位的立方和等于该数本身，例如153

public class Flower {

        public static void main(String[] args) {
//            遍历所有三位数
            for(int n=100;n<1000;n++){
//                拿到每一位的数字
                int i=n%10;//个位数
                int k=n/100;//百位数
                int j=n/10%10;//十位数
//                判断
                if(n==i*i*i+j*j*j+k*k*k)
                {
                    System.out.println(n);
                }
            }
        }
    }

