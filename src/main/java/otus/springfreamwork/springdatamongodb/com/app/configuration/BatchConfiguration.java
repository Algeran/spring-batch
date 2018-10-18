package otus.springfreamwork.springdatamongodb.com.app.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import otus.springfreamwork.springdatamongodb.domain.dao.AuthorRepository;
import otus.springfreamwork.springdatamongodb.domain.dao.BookRepository;
import otus.springfreamwork.springdatamongodb.domain.dao.GenreRepository;
import otus.springfreamwork.springdatamongodb.domain.model.Author;
import otus.springfreamwork.springdatamongodb.domain.model.Book;
import otus.springfreamwork.springdatamongodb.domain.model.Country;
import otus.springfreamwork.springdatamongodb.domain.model.Genre;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    private final Logger logger = LoggerFactory.getLogger("Batch");

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private BookRepository bookRepository;

    @Autowired
    private GenreRepository genreRepository;

    @Autowired
    private AuthorRepository authorRepository;

    @Bean
    public FlatFileItemReader<Book> csvReader() {
        return new FlatFileItemReaderBuilder<Book>()
                .name("bookItemReader")
                .resource(new FileSystemResource("data.csv"))
                .delimited()
                .delimiter(";")
                .names(new String[]{"name", "publishedDate", "authors", "genre"})
                .fieldSetMapper(fieldSet -> {
                    Book book = new Book();
                    String name = fieldSet.readString("name");
                    book.setName(name);
                    Date publishedDate = fieldSet.readDate("publishedDate");
                    book.setPublishedDate(publishedDate);
                    String authorsLine = fieldSet.readString("authors");
                    Set<Author> authors = Arrays.stream(authorsLine.split(","))
                            .map(authorName -> {
                                String[] authorData = authorName.split(" ");
                                return new Author(authorData[0], authorData[1], Country.NONE);
                            }).collect(Collectors.toSet());
                    book.setAuthors(authors);
                    String genreLine = fieldSet.readString("genre");
                    book.setGenre(new Genre(genreLine));
                    return book;
                })
                .build();
    }

    @Bean
    public ItemProcessor csvToMongoProcessor() {
        return (ItemProcessor<Book, Book>) book -> {
            book.calculateAge();
            Genre genre = book.getGenre();
            Optional<Genre> genreFromRepo = genreRepository.findByName(genre.getName());
            if (genreFromRepo.isPresent()) {
                book.setGenre(genreFromRepo.get());
            } else {
                Genre savedGenre = genreRepository.save(genre);
                book.setGenre(savedGenre);
            }

            Set<Author> processedAuthors = book.getAuthors().stream()
                    .map(author -> {
                        Optional<Author> authorFromRepo = authorRepository.findByNameAndSurname(author.getName(), author.getSurname());
                        return authorFromRepo.orElseGet(() -> authorRepository.save(author));
                    }).collect(Collectors.toSet());
            book.setAuthors(processedAuthors);
            return book;
        };
    }

    @Bean
    public ItemWriter<Book> writerToMongo() {
        return list -> bookRepository.saveAll(list);
    }

    @Bean
    public Step step1(FlatFileItemReader csvReader, ItemProcessor csvToMongoProcessor, ItemWriter writerToMongo) {
        return stepBuilderFactory.get("step1")
                .chunk(3)
                .reader(csvReader)
                .processor(csvToMongoProcessor)
                .writer(writerToMongo)
                .listener(new ItemReadListener() {
                    public void beforeRead() { logger.info("Начало чтения"); }
                    public void afterRead(Object o) { logger.info("Конец чтения"); }
                    public void onReadError(Exception e) { logger.info("Ошибка чтения"); }
                })
                .listener(new ItemWriteListener() {
                    public void beforeWrite(List list) { logger.info("Начало записи"); }
                    public void afterWrite(List list) { logger.info("Конец записи"); }
                    public void onWriteError(Exception e, List list) { logger.info("Ошибка записи"); }
                })
                .listener(new ItemProcessListener() {
                    public void beforeProcess(Object o) {logger.info("Начало обработки");}
                    public void afterProcess(Object o, Object o2) {logger.info("Конец обработки");}
                    public void onProcessError(Object o, Exception e) {logger.info("Ошбка обработки");}
                })
                .listener(new ChunkListener() {
                    public void beforeChunk(ChunkContext chunkContext) {logger.info("Начало пачки");}
                    public void afterChunk(ChunkContext chunkContext) {logger.info("Конец пачки");}
                    public void afterChunkError(ChunkContext chunkContext) {logger.info("Ошибка пачки");}
                })
                .build();
    }

    @Bean
    public Job importBookJob(Step step1) {
        return jobBuilderFactory.get("importBookJob")
                .incrementer(new RunIdIncrementer())
                .flow(step1)
                .end()
                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {
                        logger.info("Начало job");
                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        logger.info("Конец job");
                    }
                })
                .build();
    }
}
