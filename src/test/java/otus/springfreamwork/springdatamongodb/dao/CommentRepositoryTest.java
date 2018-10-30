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
import otus.springfreamwork.springdatamongodb.domain.dao.CommentRepository;
import otus.springfreamwork.springdatamongodb.domain.model.Author;
import otus.springfreamwork.springdatamongodb.domain.model.Book;
import otus.springfreamwork.springdatamongodb.domain.model.Comment;
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
public class CommentRepositoryTest {

    @Autowired
    private CommentRepository commentRepository;

    @Autowired
    private MongoTemplate mongoTemplate;
    
    @Test
    public void commentRepositoryShouldInsertEntity() {
        Comment comment = new Comment("user", "so good");
        commentRepository.save(comment);

        Query query = new Query();
        query.addCriteria(
                Criteria.where("username").is(comment.getUsername())
                        .and("comment").is(comment.getComment())
        );
        List<Comment> comments = mongoTemplate.find(query, Comment.class);

        assertFalse(comments.isEmpty());
        assertTrue(comments.contains(comment));
    }

    @Test
    public void commentRepositoryShouldGetCommentById() {
        Comment comment = new Comment("user", "so good");

        mongoTemplate.save(comment);

        Optional<Comment> commentFromRepo = commentRepository.findById(comment.getId());

        assertTrue(commentFromRepo.isPresent());
        assertEquals(comment, commentFromRepo.get());
    }

    @Test
    public void commentRepositoryShouldGetCommentByUsername() {
        Comment comment = new Comment("user", "so good");

        mongoTemplate.save(comment);

        List<Comment> comments = commentRepository.findAllByUsername(comment.getUsername());

        assertFalse(comments.isEmpty());
        assertTrue(comments.contains(comment));
    }

    @Test
    public void commentRepositoryShouldGetAllComments() {
        Comment comment = new Comment("user", "so good");
        Comment comment_2 = new Comment("user2", "so bad");

        mongoTemplate.save(comment);
        mongoTemplate.save(comment_2);

        List<Comment> comments = commentRepository.findAll();

        assertFalse(comments.isEmpty());
        assertTrue(comments.contains(comment));
        assertTrue(comments.contains(comment_2));
    }

    @Test
    public void commentRepositoryShouldDeleteCommentById() {
        Comment comment = new Comment("user", "so good");

        mongoTemplate.save(comment);

        commentRepository.deleteById(comment.getId());

        Optional<Comment> commentFromRepo = commentRepository.findById(comment.getId());

        assertFalse(commentFromRepo.isPresent());
    }

    @Test
    public void commentRepositoryShouldDeleteCommentByName() {
        Comment comment = new Comment("user", "so good");

        mongoTemplate.save(comment);

        commentRepository.deleteByUsername(comment.getUsername());

        List<Comment> comments = commentRepository.findAllByUsername(comment.getUsername());

        assertTrue(comments.isEmpty());
    }

    @Test
    public void commentRepositoryShouldReturnCount_2() {
        Comment comment = new Comment("user", "so good");
        Comment comment_2 = new Comment("user2", "so bad");

        long countBefore = commentRepository.count();

        mongoTemplate.save(comment);
        mongoTemplate.save(comment_2);

        long count = commentRepository.count();

        assertEquals(2, count - countBefore);
    }

    @Test
    public void commentRepositoryShouldReturnCommentsByBookName() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Book book = new Book("War And Piece", new Date(), parts, Collections.singleton(author), genre);

        Comment comment = new Comment("user", "so good");
        comment.setBooks(Collections.singleton(book));

        mongoTemplate.save(author);
        mongoTemplate.save(genre);
        mongoTemplate.save(book);
        mongoTemplate.save(comment);


        List<Comment> comments = commentRepository.getByBookId(new ObjectId(book.getId()));

        assertFalse(comments.isEmpty());
        assertTrue(comments.contains(comment));
    }
}