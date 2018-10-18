package otus.springfreamwork.springdatamongodb.com.app.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import otus.springfreamwork.springdatamongodb.domain.dao.AuthorRepository;
import otus.springfreamwork.springdatamongodb.domain.dao.BookRepository;
import otus.springfreamwork.springdatamongodb.domain.dao.CommentRepository;
import otus.springfreamwork.springdatamongodb.domain.dao.GenreRepository;

import javax.annotation.PostConstruct;

@Service
public class DBCleaner {

    @Autowired
    private BookRepository bookRepository;

    @Autowired
    private AuthorRepository authorRepository;

    @Autowired
    private GenreRepository genreRepository;

    @Autowired
    private CommentRepository commentRepository;

    @PostConstruct
    public void cleanDB() {
        commentRepository.deleteAll();
        bookRepository.deleteAll();
        authorRepository.deleteAll();
        genreRepository.deleteAll();
    }
}
