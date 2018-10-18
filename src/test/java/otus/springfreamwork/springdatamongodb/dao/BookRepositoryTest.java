package otus.springfreamwork.springdatamongodb.dao;

import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import otus.springfreamwork.springdatamongodb.domain.dao.BookRepository;
import otus.springfreamwork.springdatamongodb.domain.model.Author;
import otus.springfreamwork.springdatamongodb.domain.model.Book;
import otus.springfreamwork.springdatamongodb.domain.model.Genre;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD;
import static otus.springfreamwork.springdatamongodb.domain.model.Country.RUSSIA;

@SpringBootTest
@RunWith(SpringRunner.class)
@DirtiesContext(classMode = BEFORE_EACH_TEST_METHOD)
public class BookRepositoryTest {

    @Autowired
    private BookRepository bookRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Test
    public void bookRepositoryShouldInsertEntity() {
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Book book = new Book("War And Piece", new Date(), parts, Collections.singleton(author), genre);
        mongoTemplate.save(author);
        mongoTemplate.save(genre);

        bookRepository.save(book);

        Query query = new Query();
        query.addCriteria(Criteria.where("name").is(book.getName()));
        List<Book> books = mongoTemplate.find(query, Book.class);

        assertFalse(books.isEmpty());
        assertTrue(books.contains(book));
    }

    @Test
    public void bookRepositoryShouldGetBookById() {
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Book book = new Book("War And Piece", new Date(), parts, Collections.singleton(author), genre);

        mongoTemplate.save(author);
        mongoTemplate.save(genre);
        mongoTemplate.save(book);

        Optional<Book> bookFromRepo = bookRepository.findById(book.getId());

        assertTrue(bookFromRepo.isPresent());
        assertEquals(book, bookFromRepo.get());
    }

    @Test
    public void bookRepositoryShouldGetBookByName() {
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Book book = new Book("War And Piece", new Date(), parts, Collections.singleton(author), genre);

        mongoTemplate.save(author);
        mongoTemplate.save(genre);
        mongoTemplate.save(book);

        Optional<Book> bookFromRepo = bookRepository.findByName(book.getName());

        assertTrue(bookFromRepo.isPresent());
        assertEquals(book, bookFromRepo.get());
    }

    @Test
    public void bookRepositoryShouldGetAllBooks() {
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Book book = new Book("War And Piece", new Date(), parts, Collections.singleton(author), genre);
        Book book_2 = new Book("Anna Karenina", new Date(), parts, Collections.singleton(author), genre);

        mongoTemplate.save(author);
        mongoTemplate.save(genre);
        mongoTemplate.save(book);
        mongoTemplate.save(book_2);

        List<Book> books = bookRepository.findAll();

        assertFalse(books.isEmpty());
        assertTrue(books.contains(book));
        assertTrue(books.contains(book_2));
    }

    @Test
    public void bookRepositoryShouldDeleteBookById() {
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Book book = new Book("War And Piece", new Date(), parts, Collections.singleton(author), genre);

        mongoTemplate.save(author);
        mongoTemplate.save(genre);
        mongoTemplate.save(book);

        bookRepository.deleteById(book.getId());

        Optional<Book> bookFromRepo = bookRepository.findById(book.getId());

        assertFalse(bookFromRepo.isPresent());
    }

    @Test
    public void bookRepositoryShouldDeleteBookByName() {
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Book book = new Book("War And Piece", new Date(), parts, Collections.singleton(author), genre);

        mongoTemplate.save(author);
        mongoTemplate.save(genre);
        mongoTemplate.save(book);

        bookRepository.deleteByName(book.getName());

        Optional<Book> bookFromRepo = bookRepository.findByName(book.getName());

        assertFalse(bookFromRepo.isPresent());
    }

    @Test
    public void bookRepositoryShouldReturnCount_2() {
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Book book = new Book("War And Piece", new Date(), parts, Collections.singleton(author), genre);
        Book book_2 = new Book("Anna Karenina", new Date(), parts, Collections.singleton(author), genre);

        long countBefore = bookRepository.count();

        mongoTemplate.save(author);
        mongoTemplate.save(genre);
        mongoTemplate.save(book);
        mongoTemplate.save(book_2);

        long count = bookRepository.count();

        assertEquals(2, count - countBefore);
    }

    @Test
    public void bookRepositoryShouldGetBookByAuthorId() {
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Book book = new Book("War And Piece", new Date(), parts, Collections.singleton(author), genre);

        mongoTemplate.save(author);
        mongoTemplate.save(genre);
        mongoTemplate.save(book);

        List<Book> books = bookRepository.getByAuthorId(new ObjectId(author.getId()));

        assertTrue(books.contains(book));
    }

    @Test
    public void bookRepositoryShouldGetBookByGenreId() {
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Book book = new Book("War And Piece", new Date(), parts, Collections.singleton(author), genre);

        mongoTemplate.save(author);
        mongoTemplate.save(genre);
        mongoTemplate.save(book);

        List<Book> books = bookRepository.getByGenreId(new ObjectId(genre.getId()));

        assertTrue(books.contains(book));
    }
}