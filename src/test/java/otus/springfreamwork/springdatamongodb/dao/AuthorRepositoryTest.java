package otus.springfreamwork.springdatamongodb.dao;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import otus.springfreamwork.springdatamongodb.domain.dao.AuthorRepository;
import otus.springfreamwork.springdatamongodb.domain.model.Author;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD;
import static otus.springfreamwork.springdatamongodb.domain.model.Country.RUSSIA;

@SpringBootTest
@RunWith(SpringRunner.class)
@DirtiesContext(classMode = BEFORE_EACH_TEST_METHOD)
public class AuthorRepositoryTest {

    @Autowired
    private AuthorRepository authorRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Test
    public void authorRepositoryShouldInsertEntity() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        authorRepository.save(author);

        Query query = new Query();
        query.addCriteria(
                Criteria.where("name").is(author.getName())
                        .and("surname").is(author.getSurname()));
        List<Author> authors = mongoTemplate.find(query, Author.class);

        assertFalse(authors.isEmpty());
        assertTrue(authors.contains(author));
    }

    @Test
    public void authorRepositoryShouldGetAuthorById() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);

        mongoTemplate.save(author);

        Optional<Author> authorFromRepo = authorRepository.findById(author.getId());

        assertTrue(authorFromRepo.isPresent());
        assertEquals(author, authorFromRepo.get());
    }

    @Test
    public void authorRepositoryShouldGetAuthorByNameAndSurname() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);

        mongoTemplate.save(author);

        Optional<Author> authorFromRepo = authorRepository.findByNameAndSurname("Leo", "Tolstoy");

        assertTrue(authorFromRepo.isPresent());
        assertEquals(author, authorFromRepo.get());
    }

    @Test
    public void authorRepositoryShouldGetAllAuthors() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Author author_2 = new Author("Fyodor", "Dostoevsky", RUSSIA);

        mongoTemplate.save(author);
        mongoTemplate.save(author_2);

        List<Author> authors = authorRepository.findAll();

        assertFalse(authors.isEmpty());
        assertEquals(2, authors.size());
        assertTrue(authors.contains(author));
        assertTrue(authors.contains(author_2));
    }

    @Test
    public void authorRepositoryShouldDeleteAuthorById() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);

        mongoTemplate.save(author);

        authorRepository.deleteById(author.getId());

        Optional<Author> authorFromRepo = authorRepository.findById(author.getId());
        assertFalse(authorFromRepo.isPresent());
    }

    @Test
    public void authorRepositoryShouldDeleteAuthorByNameAndSurname() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);

        mongoTemplate.save(author);

        authorRepository.deleteByNameAndSurname(author.getName(), author.getSurname());

        Optional<Author> authorFromRepo = authorRepository.findByNameAndSurname("Leo", "Tolstoy");
        assertFalse(authorFromRepo.isPresent());
    }

    @Test
    public void authorRepositoryShouldReturnCount_2() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Author author_2 = new Author("Fyodor", "Dostoevsky", RUSSIA);

        long countBefore = authorRepository.count();

        mongoTemplate.save(author);
        mongoTemplate.save(author_2);

        long count = authorRepository.count();

        assertEquals(2, count - countBefore);
    }
}