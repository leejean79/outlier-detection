package com.leejean.counseling;

class Transport{//交通工具类
    int speed;
    String name;
    public Transport(String name,int speed){//构造方法
        this.speed=speed;
        this.name=name;
    }
    public void run(){//交通工具运行
        System.out.println("交通工具在运行！");
    }
}

//飞机子类
class Plane extends Transport{
    //构造方法
    Plane(String name,int speed){
        //调用父类构造方法
        super(name,speed);
    }
    public void run(){
        //重写父类方法
        System.out.println(name+"飞机以"+speed+"km/h的速度在空中飞行。");
    }
}


public class Java03MyInheritanceDemo2 {
    public static void main(String args[])	{
        //声明一个交通工具类的对象
        Transport aTransport;
        //赋值兼容规则，生成一个飞机对象
        aTransport = new Plane("战斗机",200);
        //调用run方法
        aTransport.run();
    }
}
