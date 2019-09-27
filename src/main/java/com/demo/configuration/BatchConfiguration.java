package com.demo.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import com.demo.model.Student;

@Configuration
public class BatchConfiguration {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private ItemReader<Student> itemReader;

	@Autowired
	private ItemWriter<Student> itemWriter;

	@Autowired
	private ItemProcessor<Student, Student> itemProcessor;

	@Autowired
	private Step step;

	@Bean
	public FlatFileItemReader<Student> reader() {

		// Create reader instance
		FlatFileItemReader<Student> reader = new FlatFileItemReader<Student>();

		// Set input file location
		reader.setResource(new ClassPathResource("inbound/Student.txt"));

		// Configure how each line will be parsed and mapped to different values
		reader.setLineMapper(new DefaultLineMapper<Student>() {
			{
				// 3 columns in each row
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames(new String[] { "name", "age", "id" });
					}
				});
				// Set values in Student class
				setFieldSetMapper(new BeanWrapperFieldSetMapper<Student>() {
					{
						setTargetType(Student.class);
					}
				});
			}
		});
		return reader;
	}

	@Bean
	public FlatFileItemWriter<Student> flatFileItemWriter() {

		// Create writer instance
		FlatFileItemWriter<Student> writer = new FlatFileItemWriter<Student>();
		
		// Set output file location
		writer.setResource(new FileSystemResource("outbound/Student-out.txt"));
		
		//Set pipe delimiter for fields
		DelimitedLineAggregator<Student> aggregator = new DelimitedLineAggregator<Student>();
		aggregator.setDelimiter("|");

		
		//Create fields from object
		BeanWrapperFieldExtractor<Student> extractor = new BeanWrapperFieldExtractor<>();
		extractor.setNames(new String[] { "name", "age", "id" });
		
		//Aggregate the fields with the delimiter between fileds
		aggregator.setFieldExtractor(extractor);

		writer.setLineAggregator(aggregator);

		return writer;

	}

	@Bean
	public Step batchStep() {
		return stepBuilderFactory.get("batchStep").<Student, Student>chunk(2).reader(itemReader)
				.processor(itemProcessor).writer(itemWriter).build();
	}

	@Bean
	public Job batchJob() {
		return jobBuilderFactory.get("batchJob").start(step).build();
	}

}
