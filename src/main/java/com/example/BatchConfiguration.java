package com.example;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Bean
	public ItemReader<Person> reader(){
		FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
		reader.setResource(new ClassPathResource("sample-data.csv"));
		reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[] { "firstName", "lastName" });
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
		reader.setLinesToSkip(1);
        return reader;
	}
	
	@Bean
    public ItemProcessor<Person, Person> processor() {
        return new PersonItemProcessor();
    }
	
	@Bean
	public ItemWriter<Person> writer(DataSource dataSource){
		FlatFileItemWriter<Person> writer = new FlatFileItemWriter<Person>();
		writer.setResource(new FileSystemResource("C:\\Users\\belkap\\output.json"));
		writer.setLineAggregator(new LineAggregator<Person>() {
			
			@Override
			public String aggregate(Person item) {
				// TODO Auto-generated method stub
				return item.getFirstName();
			}
		});
		return writer;
	}
	
	@Bean
	public Job importUserJob(JobBuilderFactory jobs, Step s1, JobExecutionListener listener){
		return jobs.get("importUserJob")
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.flow(s1)
				.end()
				.build();
	}
	
	@Bean
	public Step step1(StepBuilderFactory stepBuilderFactory, ItemReader<Person> reader,
    ItemWriter<Person> writer, ItemProcessor<Person, Person> processor) {
		 return stepBuilderFactory.get("step1")
				 .<Person, Person>chunk(10)
				 .reader(reader)
				 .processor(processor)
				 .writer(writer)
				 .build();
	}
	
}
