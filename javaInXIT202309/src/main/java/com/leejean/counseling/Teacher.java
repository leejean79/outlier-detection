package com.leejean.counseling;

public class Teacher {

    int id;
    String sub;
    String name;
    Boolean gender;

    public Teacher(int id, String sub, String name, Boolean gender) {
        this.id = id;
        this.sub = sub;
        this.name = name;
        this.gender = gender;
    }

    @Override
    public String toString() {
        return "Teacher{" +
                "id=" + id +
                ", sub='" + sub + '\'' +
                ", name='" + name + '\'' +
                ", gender=" + gender +
                '}';
    }
}
