package com.cognizant.arrays;

public class ArrayTest1 {

    public static void main(String[] args) {
            // TODO Auto-generated method stub

            int a[]=new int[10];
            for(int i=0;i<a.length;i++) {
                    System.out.println(a[i]);
            }
            
            System.out.println("enhanced for loop");
            for(int x:a) {
                    System.out.println(x);
            }
    }

}

