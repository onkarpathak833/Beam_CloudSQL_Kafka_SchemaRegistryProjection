package com.beam.examples.domain;

public class Customer {
    int id;
    String name;
    int age;
    String gender;

    public Customer(int id, String name, int age, String gender, boolean isPrime) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.gender = gender;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", gender='" + gender + '\'' +
                '}';
    }

//    @Override
//    public void put(int i, Object v) {
//
//    }
//
//    @Override
//    public Object get(int i) {
//        return null;
//    }
//
//    @Override
//    public Schema getSchema() {
//        return null;
//    }
}
