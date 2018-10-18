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
import otus.springfreamwork.springdatamongodb.domain.dao.GenreRepository;
import otus.springfreamwork.springdatamongodb.domain.model.Genre;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD;

@RunWith(SpringRunner.class)
@DirtiesContext(classMode = BEFORE_EACH_TEST_METHOD)
@SpringBootTest
public class GenreRepositoryTest {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private GenreRepository genreRepository;

    @Test
    public void genreRepositoryShouldInsertEntity() {
        Genre genre = new Genre("fantasy");
        genreRepository.save(genre);

        Query query = new Query();
        query.addCriteria(Criteria.where("name").is(genre.getName()));
        List<Genre> genres = mongoTemplate.find(query, Genre.class);

        assertFalse(genres.isEmpty());
        assertTrue(genres.contains(genre));
    }

    @Test
    public void genreRepositoryShouldGetGenreById() {
        Genre genre = new Genre("fantasy");

        mongoTemplate.save(genre);

        Optional<Genre> genreFromRepository = genreRepository.findById(genre.getId());

        assertTrue(genreFromRepository.isPresent());
        assertEquals(genre, genreFromRepository.get());
    }

    @Test
    public void genreRepositoryShouldGetGenreByName() {
        Genre genre = new Genre("fantasy");

        mongoTemplate.save(genre);

        Optional<Genre> genreFromRepository = genreRepository.findByName(genre.getName());

        assertTrue(genreFromRepository.isPresent());
        assertEquals(genre, genreFromRepository.get());
    }

    @Test
    public void genreRepositoryShouldGetAllGenres() {
        Genre genre = new Genre("fantasy");
        Genre genre_2 = new Genre("novel");

        mongoTemplate.save(genre);
        mongoTemplate.save(genre_2);

        List<Genre> genres = genreRepository.findAll();

        assertFalse(genres.isEmpty());
        assertEquals(2, genres.size());
        assertTrue(genres.contains(genre));
        assertTrue(genres.contains(genre_2));
    }

    @Test
    public void genreRepositoryShouldDeleteGenreById() {
        Genre genre = new Genre("fantasy");

        mongoTemplate.save(genre);
        String id = genre.getId();

        genreRepository.deleteById(id);

        Optional<Genre> genreFromRepository = genreRepository.findById(id);

        assertFalse(genreFromRepository.isPresent());
    }

    @Test
    public void genreRepositoryShouldDeleteGenreByName() {
        Genre genre = new Genre("fantasy");

        mongoTemplate.save(genre);

        genreRepository.deleteByName(genre.getName());

        Optional<Genre> genreFromRepository = genreRepository.findByName(genre.getName());

        assertFalse(genreFromRepository.isPresent());
    }

    @Test
    public void genreRepositoryShouldReturnCount_2() {
        Genre genre = new Genre("fantasy");
        Genre genre_2 = new Genre("novel");

        long countBefore = genreRepository.count();

        mongoTemplate.save(genre);
        mongoTemplate.save(genre_2);

        long count = genreRepository.count();

        assertEquals(2, count - countBefore);
    }
}