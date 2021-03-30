package cn.jaychang.flink.learn.common.model;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Student {
    private Long id;
    private String name;

    private Integer age;

    public Student(Long id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public Student() {

    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

    public static void main(String[] args) {
        Student s1 = new Student(1L, "JAY", 10);
        Student s2 = new Student(2L, "kAY", 12);
        Student s3 = new Student(2L, "MAY", 13);
        Student s4 = new Student(5L, "DAY", 15);
        Student s5 = new Student(2L, "CAY", 13);
        Student s6 = new Student(2L, "CAY", null);
        List<Student> students = Arrays.asList(s1, s2, s3, s4, s5,s6);

        List<Student> studentList = students.stream().sorted(Comparator.comparing(Student::getId).thenComparing(Comparator.comparing(Student::getAge,Comparator.nullsFirst(Integer::compareTo)).reversed()).thenComparing(Comparator.comparing(Student::getName))).collect(Collectors.toList());

        System.out.println(studentList);


    }
}
