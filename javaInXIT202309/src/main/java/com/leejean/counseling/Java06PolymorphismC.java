package com.leejean.counseling;

class PetB{ }

class CatB extends PetB{ }

public class Java06PolymorphismC {

    public static void main(String[] args) {

        int x = 100;

        PetB p = new PetB();

        CatB c = null;

        PetB p1 = new CatB();

        PetB c1 = new PetB();

        if(p instanceof PetB)x+=100;

        if(c instanceof CatB)x+=200;

        if(p1 instanceof CatB)x+=300;

        if(c1 instanceof CatB)x+=400;

        System.out.println(x);
    }

}
