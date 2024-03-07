package com.leejean.counseling.day0925;

abstract class Animal {
    // 抽象方法，没有具体的实现
    public abstract void makeSound();

    // 具体方法，有实现代码
    public void sleep() {
        System.out.println("Animal is sleeping");
    }
}

class Dog extends Animal {
    // 实现抽象方法
    public void makeSound() {
        System.out.println("Dog barks");
    }
}


public class MyMain {

    public static void main(String[] args) {
        Dog dog = new Dog();
        dog.makeSound();  // 输出: ？
        dog.sleep();      // 输出: ？
    }
}
