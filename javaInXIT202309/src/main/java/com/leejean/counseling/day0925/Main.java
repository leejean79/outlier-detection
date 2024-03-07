package com.leejean.counseling.day0925;

class Animal {
    public void eat() {
        System.out.println("Animal is eating.");
    }
}
class Dog extends Animal {
    public void bark() {
        System.out.println("Dog is barking.");
    }
}


public class Main {

    public static void main(String[] args) {
        Animal animal = new Dog();
        if (animal instanceof Dog) {
            Dog dog = (Dog) animal; // 向下转型
            dog.bark(); // 可以调⽤⼦类特有的⽅法
        }
    }
}
