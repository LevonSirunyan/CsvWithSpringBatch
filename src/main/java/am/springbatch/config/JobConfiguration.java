package am.springbatch.config;

import am.springbatch.entity.Person;
import am.springbatch.reader.MyCustomReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Configuration
@EnableBatchProcessing
public class JobConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;

    @Autowired
    public MyCustomReader myCustomReader;

    @Bean
    public ItemReader<Person> itemReader() throws IOException, URISyntaxException {
        MultiResourceItemReader<Person> reader = new MultiResourceItemReader<>();
        URL res = getClass().getClassLoader().getResource("data.zip");
        File file = Paths.get(res.toURI()).toFile();
        ZipFile zipFile = new ZipFile(file.getAbsolutePath());
        reader.setComparator(Comparator.comparing(Resource::getDescription));
        reader.setResources(extractFiles(zipFile));
        reader.setDelegate(myCustomReader);
        return reader;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Bean
    public JdbcBatchItemWriter<Person> itemWriter() {
        JdbcBatchItemWriter<Person> itemWriter = new JdbcBatchItemWriter<>();

        itemWriter.setDataSource(this.dataSource);
        itemWriter.setSql("INSERT INTO Person (first_name, last_name, csv_date)VALUES (:firstName, :lastName, :csvDate)");

        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider());
        itemWriter.afterPropertiesSet();

        return itemWriter;
    }

    @Bean
    public Step step1() throws IOException, URISyntaxException {
        return stepBuilderFactory.get("step1")
                .<Person, Person>chunk(10)
                .reader(itemReader())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Job job() throws IOException, URISyntaxException {
        return jobBuilderFactory.get("MyJob")
                .incrementer(new RunIdIncrementer())
                .flow(step1()).end().build();
    }


    public static Resource[] extractFiles(final ZipFile currentZipFile) throws IOException {
        List<Resource> extractedResources = new ArrayList<>();
        Enumeration<? extends ZipEntry> zipEntryEnum = currentZipFile.entries();
        while (zipEntryEnum.hasMoreElements()) {
            ZipEntry zipEntry = zipEntryEnum.nextElement();
            if (!zipEntry.isDirectory()) {
                extractedResources.add(
                        new InputStreamResource(
                                currentZipFile.getInputStream(zipEntry),
                                zipEntry.getName()));
            }
        }
        Resource[] retResources = new Resource[extractedResources.size()];
        extractedResources.toArray(retResources);
        return retResources;
    }
}