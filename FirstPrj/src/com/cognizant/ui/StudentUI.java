package com.cognizant.ui;

import com.cognizant.data.Student;
import com.cognizant.middleware.KafkaService;
import com.cognizant.service.StudentService;

public class StudentUI {
        public static void main(String[] args) {
                StudentService service=new StudentService();
                Student student=service.getStudentData();
                student.print();
                
                
                KafkaService KafkaService = new KafkaService();
                KafkaService.printDetails();
        }

}
