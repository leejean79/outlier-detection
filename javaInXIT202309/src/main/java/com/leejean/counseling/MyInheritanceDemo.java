package com.leejean.counseling;

//定义一个父类Person
class Person{
    public String name;
    public int age;
    public float salry;

    public void say(){}

    public void eat(){
        System.out.println("正在吃饭....");
    }
}

/** 定义一个子类 ChinesePerson **/
class ChinesePerson extends Person{

    // 公共属性和方法就不需要重新定义了（父类中的私有属性和方法不会被继承）

    // 可以定义特有属性
    public String friends;

    // 可以定义特有方法
    public void makeFriends(String friends){
        this.friends = friends;
    }

    // 也可以重写父类中的方法
    @Override
    public void eat(){
        System.out.println("你好，吃了吗？");
    }

}



public class MyInheritanceDemo {

    public static void main(String[] args) {

        ChinesePerson chinesePerson = new ChinesePerson();

        chinesePerson.name = "lijing";
        chinesePerson.eat();

        chinesePerson.makeFriends("jully");

        System.out.println("my best friends is: " + chinesePerson.friends);
    }
}
