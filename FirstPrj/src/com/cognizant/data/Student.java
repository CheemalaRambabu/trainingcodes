package com.cognizant.data;

public class Student {
    private int rollno;
    private String name;
    public Student(int rollno, String name) {
            
            this.rollno = rollno;
            this.name = name;
    }
    
    public void print() {
            System.out.println("Rollno:"+rollno);
            System.out.println("Name:"+name);
    }

}





