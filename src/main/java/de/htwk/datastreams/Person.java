package de.htwk.datastreams;

public class Person {
    private String name;
    private Integer age;
    public Person() {}

    public Person(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    public String toString() {
        return this.name.toString() + ": age " + this.age.toString();
    }

    public String getName() {
        return name;
    }

    public Integer getAge() {
        return age;
    }
}
