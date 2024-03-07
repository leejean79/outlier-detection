package com.leejean.counseling.day0925;

class CarB{
    CarB(){System.out.print("A");}
    static{System.out.print("B");}
    CarB(int a){System.out.print("C");}
    {System.out.print("D");}
}

class TrunkB extends CarB{
    TrunkB(){
        super(5);
        System.out.print("E");
    }
    static{System.out.print("F");}
    TrunkB(int a){System.out.print("G");}
    {System.out.print("H");}
}

public class MyInitialClassDemo {

    public static void main(String[] args) {
        TrunkB s1=new TrunkB();
        System.out.print("I");
        TrunkB s2=new TrunkB(2);
        System.out.print("J");
    }
}
//result: