package com.demo.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.demo.model.Student;

@Component
public class StudentProcessor implements ItemProcessor<Student, Student> {

	@Override
	public Student process(Student student) throws Exception {
		student.setName(student.getName().toUpperCase());
		student.setId(student.getId() + "SCH-0009");
		return student;
	}

}
