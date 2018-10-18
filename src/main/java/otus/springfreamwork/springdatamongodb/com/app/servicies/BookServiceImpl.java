package otus.springfreamwork.springdatamongodb.com.app.servicies;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import otus.springfreamwork.springdatamongodb.domain.app.services.BookService;
import otus.springfreamwork.springdatamongodb.domain.dao.AuthorRepository;
import otus.springfreamwork.springdatamongodb.domain.dao.BookRepository;
import otus.springfreamwork.springdatamongodb.domain.dao.CommentRepository;
import otus.springfreamwork.springdatamongodb.domain.dao.GenreRepository;
import otus.springfreamwork.springdatamongodb.domain.model.Author;
import otus.springfreamwork.springdatamongodb.domain.model.Book;
import otus.springfreamwork.springdatamongodb.domain.model.Comment;
import otus.springfreamwork.springdatamongodb.domain.model.Genre;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static otus.springfreamwork.springdatamongodb.domain.model.Country.RUSSIA;

@Service
public class BookServiceImpl implements BookService {
    private final BookRepository bookRepository;
    private final AuthorRepository authorRepository;
    private final GenreRepository genreRepository;
    private final CommentRepository commentRepository;

    @Autowired
    public BookServiceImpl(
            BookRepository bookRepository,
            AuthorRepository authorRepository,
            GenreRepository genreRepository,
            CommentRepository commentRepository
    ) {
        this.bookRepository = bookRepository;
        this.authorRepository = authorRepository;
        this.genreRepository = genreRepository;
        this.commentRepository = commentRepository;
    }

    @Override
    public String createBookByNameAndAuthorAndGenre(String bookName, String name, String surname, String genreName) {

        Optional<Book> book = bookRepository.findByName(bookName);
        if (book.isPresent()) {
            return "Книга уже в базе";
        }

        StringBuilder result = new StringBuilder();
        Author author = authorRepository.findByNameAndSurname(name, surname).orElse(null);
        if (author == null) {
            author = new Author(name, surname, RUSSIA);
            result.append("Создан автор").append("\n");
        }

        Genre genre = genreRepository.findByName(genreName).orElse(null);
        if (genre == null) {
            genre = new Genre(genreName);
            result.append("Создан жанр").append("\n");
        }

        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        bookRepository.save(new Book(bookName, new Date(2018, 4, 10), parts, Collections.singleton(author), genre));
        return result.append("Книга успешно создана").toString();
    }

    @Override
    public String getAllBooks() {
        List<Book> books = bookRepository.findAll();
        StringBuilder stringBuilder = new StringBuilder();
        if (books.isEmpty()) {
            stringBuilder.append("Нет книг в базе");
        } else {
            stringBuilder.append("Список книг:");
            books.forEach(book -> stringBuilder.append("\n").append(book));
        }
        return stringBuilder.toString();
    }

    @Override
    public String countBooks() {
        return "Количество книг в базе: " + bookRepository.count();
    }

    @Override
    public String deleteBook(String name) {
        Book book = bookRepository.findByName(name).orElse(null);
        String result;
        if (book != null) {
            bookRepository.deleteByName(name);
            result = "Книга успешно удалена";
        } else {
            result = "Не найдено книги в базе для удаления";
        }
        return result;
    }

    @Override
    public String getBook(String name) {
        Book book = bookRepository.findByName(name).orElse(null);
        String result;
        if (book == null) {
            result = "Не найдено книги в базе";
        } else {
            result = "Найдена книга: " + book;
        }
        return result;
    }

    @Override
    public String getBooksByAuthorNameAndSurname(String name, String surname) {
        StringBuilder stringBuilder = new StringBuilder();
        Author author = authorRepository.findByNameAndSurname(name, surname).orElse(null);
        if (author == null) {
            stringBuilder.append("В базе нет такого автора");
        } else {
            List<Book> books = bookRepository.getByAuthorId(new ObjectId(author.getId()));
            if (books.isEmpty()) {
                stringBuilder.append("Список книг для выбранного автора пуст");
            } else {
                stringBuilder.append("Список книг для автора:");
                books.forEach(book -> stringBuilder.append("\n").append(book));
            }
        }
        return stringBuilder.toString();
    }

    @Override
    public String getBooksByGenreName(String name) {
        StringBuilder stringBuilder = new StringBuilder();
        Genre genre = genreRepository.findByName(name).orElse(null);
        if (genre == null) {
            stringBuilder.append("В базе нет такого жанра");
        } else {
            List<Book> books = bookRepository.getByGenreId(new ObjectId(genre.getId()));
            if (books.isEmpty()) {
                stringBuilder.append("Список книг для выбранного жанра пуст");
            } else {
                stringBuilder.append("Список книг для жанра:");
                books.forEach(book -> stringBuilder.append("\n").append(book));
            }
        }
        return stringBuilder.toString();
    }

    @Override
    public String getCommentsOnBook(String name) {
        StringBuilder stringBuilder = new StringBuilder();
        Optional<Book> book = bookRepository.findByName(name);
        if (book.isPresent()) {
            List<Comment> comments = commentRepository.getByBookId(new ObjectId(book.get().getId()));
            if (comments.isEmpty()) {
                stringBuilder.append("Нет комментариев на книгу");
            } else {
                stringBuilder.append("Список комментариев для книги:");
                comments.forEach(comment -> stringBuilder.append("\n").append(comment));
            }
        } else {
            stringBuilder.append("Нет комментариев на книгу");
        }

        return stringBuilder.toString();
    }
}
