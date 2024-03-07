package com.leejean.counseling;


class Test_abc{	int x,y;}

public class ParameterTransA {
    public static void function(int m,Test_abc a1,int[] y){//第三行
        m=100;
        a1.y=150;
        y[1]=200;
    }
    public static void main(String[] args) {
        int a=10;
        Test_abc c = new Test_abc();
        c.x=20;c.y=30;
        int[] arr={40,50,60};
        function(a,c,arr);
        System.out.print(a+" ");
        System.out.print(c.x+" "+c.y+" ");
        System.out.print(arr[0]+" "+arr[1]);
    }

}
