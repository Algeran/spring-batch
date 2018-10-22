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
import org.springframework.batch.core.annotation.AfterJob;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeJob;
import org.springframework.batch.core.annotation.BeforeStep;
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
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
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
    public FlatFileItemReader<Set<Author>> authorReader() {
        return new FlatFileItemReaderBuilder<Set<Author>>()
                .name("authorItemReader")
                .resource(new FileSystemResource("data.csv"))
                .delimited()
                .delimiter(";")
                .names(new String[]{"name", "publishedDate", "authors", "genre"})
                .fieldSetMapper(fieldSet -> {
                    String authorsLine = fieldSet.readString("authors");
                    Set<Author> authors = Arrays.stream(authorsLine.split(","))
                            .map(authorName -> {
                                String[] authorData = authorName.trim().split(" ");
                                return new Author(authorData[0], authorData[1], Country.NONE);
                            }).collect(Collectors.toSet());
                    return authors;
                })
                .build();
    }

    @Bean
    public ItemProcessor<Set<Author>, Set<Author>> filterDuplicateAuthors() {
        return new ItemProcessor<Set<Author>, Set<Author>>() {

            private Set<Author> uniqueAuthors;

            @Override
            public Set<Author> process(Set<Author> authors) throws Exception {
                authors = authors.stream()
                        .filter(author -> !authorRepository.findByNameAndSurname(author.getName(), author.getSurname()).isPresent())
                        .collect(Collectors.toSet());
                authors.removeAll(uniqueAuthors);
                uniqueAuthors.addAll(authors);
                return authors;
            }

            @BeforeStep
            public void initList() {
                uniqueAuthors = Collections.synchronizedSet(new HashSet<>());
            }

            @AfterStep
            public void clearList() {
                uniqueAuthors.clear();
            }
        };
    }

    @Bean
    public ItemWriter<Set<Author>> writeAuthorsToDB() {
        return list -> {
            Set<Author> combinedAuthors = list.stream()
                    .flatMap(authors -> authors.stream())
                    .collect(Collectors.toSet());
            authorRepository.saveAll(combinedAuthors);
        };
    }

    @Bean
    public Step step1(FlatFileItemReader authorReader, ItemProcessor filterDuplicateAuthors, ItemWriter writeAuthorsToDB) {
        return stepBuilderFactory.get("step1")
                .chunk(3)
                .reader(authorReader)
                .processor(filterDuplicateAuthors)
                .writer(writeAuthorsToDB)
                .listener(new ItemReadListener() {
                    public void beforeRead() {
                        logger.info("Начало чтения авторов");
                    }

                    public void afterRead(Object o) {
                        logger.info("Конец чтения авторов");
                    }

                    public void onReadError(Exception e) {
                        logger.info("Ошибка чтения авторов");
                    }
                })
                .listener(new ItemWriteListener() {
                    public void beforeWrite(List list) {
                        logger.info("Начало записи авторов");
                    }

                    public void afterWrite(List list) {
                        logger.info("Конец записи авторов");
                    }

                    public void onWriteError(Exception e, List list) {
                        logger.info("Ошибка записи авторов");
                    }
                })
                .listener(new ItemProcessListener() {
                    public void beforeProcess(Object o) {
                        logger.info("Начало обработки авторов");
                    }

                    public void afterProcess(Object o, Object o2) {
                        logger.info("Конец обработки авторов");
                    }

                    public void onProcessError(Object o, Exception e) {
                        logger.info("Ошбка обработки авторов");
                    }
                })
                .listener(new ChunkListener() {
                    public void beforeChunk(ChunkContext chunkContext) {
                        logger.info("Начало пачки авторов");
                    }

                    public void afterChunk(ChunkContext chunkContext) {
                        logger.info("Конец пачки авторов");
                    }

                    public void afterChunkError(ChunkContext chunkContext) {
                        logger.info("Ошибка пачки авторов");
                    }
                })
                .build();
    }

    @Bean
    public FlatFileItemReader<Genre> genreReader() {
        return new FlatFileItemReaderBuilder<Genre>()
                .name("genreItemReader")
                .resource(new FileSystemResource("data.csv"))
                .delimited()
                .delimiter(";")
                .names(new String[]{"name", "publishedDate", "authors", "genre"})
                .fieldSetMapper(fieldSet -> {
                    String genreLine = fieldSet.readString("genre");
                    return new Genre(genreLine);
                })
                .build();
    }

    @Bean
    public ItemProcessor<Genre, Genre> filterDuplicateGenres() {
        return new ItemProcessor<Genre, Genre>() {

            private Set<Genre> uniqueGenres;

            @Override
            public Genre process(Genre genre) throws Exception {
                if (genreRepository.findByName(genre.getName()).isPresent() || uniqueGenres.contains(genre)) {
                    return null;
                } else {
                    uniqueGenres.add(genre);
                    return genre;
                }
            }

            @BeforeStep
            public void initList() {
                uniqueGenres = Collections.synchronizedSet(new HashSet<>());
            }

            @AfterStep
            public void clearList() {
                uniqueGenres.clear();
            }
        };
    }

    @Bean
    public ItemWriter<Genre> writeGenresToDB() {
        return list -> genreRepository.saveAll(list);
    }

    @Bean
    public Step step2(FlatFileItemReader genreReader, ItemProcessor filterDuplicateGenres, ItemWriter writeGenresToDB) {
        return stepBuilderFactory.get("step2")
                .chunk(3)
                .reader(genreReader)
                .processor(filterDuplicateGenres)
                .writer(writeGenresToDB)
                .listener(new ItemReadListener() {
                    public void beforeRead() {
                        logger.info("Начало чтения жанров");
                    }

                    public void afterRead(Object o) {
                        logger.info("Конец чтения жанров");
                    }

                    public void onReadError(Exception e) {
                        logger.info("Ошибка чтения жанров");
                    }
                })
                .listener(new ItemWriteListener() {
                    public void beforeWrite(List list) {
                        logger.info("Начало записи жанров");
                    }

                    public void afterWrite(List list) {
                        logger.info("Конец записи жанров");
                    }

                    public void onWriteError(Exception e, List list) {
                        logger.info("Ошибка записи жанров");
                    }
                })
                .listener(new ItemProcessListener() {
                    public void beforeProcess(Object o) {
                        logger.info("Начало обработки жанров");
                    }

                    public void afterProcess(Object o, Object o2) {
                        logger.info("Конец обработки жанров");
                    }

                    public void onProcessError(Object o, Exception e) {
                        logger.info("Ошбка обработки жанров");
                    }
                })
                .listener(new ChunkListener() {
                    public void beforeChunk(ChunkContext chunkContext) {
                        logger.info("Начало пачки жанров");
                    }

                    public void afterChunk(ChunkContext chunkContext) {
                        logger.info("Конец пачки жанров");
                    }

                    public void afterChunkError(ChunkContext chunkContext) {
                        logger.info("Ошибка пачки жанров");
                    }
                })
                .build();
    }

    @Bean
    public FlatFileItemReader<Book> bookReader() {
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
                                String[] authorData = authorName.trim().split(" ");
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
                throw new Exception("No genre in DB: " + genre.getName());
            }

            Set<Author> authorsWithId = new HashSet<>();
            for (Author author : book.getAuthors()) {
                Optional<Author> authorFromRepo = authorRepository.findByNameAndSurname(author.getName(), author.getSurname());
                Author authorWithId = authorFromRepo.orElseThrow(() -> new Exception("No author in DB: " + author));
                authorsWithId.add(authorWithId);
            }
            book.setAuthors(authorsWithId);
            return book;
        };
    }

    @Bean
    public ItemWriter<Book> writerToMongo() {
        return list -> bookRepository.saveAll(list);
    }

    @Bean
    public Step step3(FlatFileItemReader bookReader, ItemProcessor csvToMongoProcessor, ItemWriter writerToMongo) {
        return stepBuilderFactory.get("step3")
                .chunk(3)
                .reader(bookReader)
                .processor(csvToMongoProcessor)
                .writer(writerToMongo)
                .listener(new ItemReadListener() {
                    public void beforeRead() {
                        logger.info("Начало чтения книг");
                    }

                    public void afterRead(Object o) {
                        logger.info("Конец чтения книг");
                    }

                    public void onReadError(Exception e) {
                        logger.info("Ошибка чтения книг");
                    }
                })
                .listener(new ItemWriteListener() {
                    public void beforeWrite(List list) {
                        logger.info("Начало записи книг");
                    }

                    public void afterWrite(List list) {
                        logger.info("Конец записи книг");
                    }

                    public void onWriteError(Exception e, List list) {
                        logger.info("Ошибка записи книг");
                    }
                })
                .listener(new ItemProcessListener() {
                    public void beforeProcess(Object o) {
                        logger.info("Начало обработки книг");
                    }

                    public void afterProcess(Object o, Object o2) {
                        logger.info("Конец обработки книг");
                    }

                    public void onProcessError(Object o, Exception e) {
                        logger.info("Ошбка обработки книг");
                    }
                })
                .listener(new ChunkListener() {
                    public void beforeChunk(ChunkContext chunkContext) {
                        logger.info("Начало пачки книг");
                    }

                    public void afterChunk(ChunkContext chunkContext) {
                        logger.info("Конец пачки книг");
                    }

                    public void afterChunkError(ChunkContext chunkContext) {
                        logger.info("Ошибка пачки книг");
                    }
                })
                .build();
    }

    @Bean
    public Job importBookJob(Step step1, Step step2, Step step3) {
        return jobBuilderFactory.get("importBookJob")
                .incrementer(new RunIdIncrementer())
                .flow(step1)
                .next(step2)
                .next(step3)
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
