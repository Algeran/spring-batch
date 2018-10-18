package otus.springfreamwork.springdatamongodb.servicies;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import otus.springfreamwork.springdatamongodb.com.app.servicies.BookServiceImpl;
import otus.springfreamwork.springdatamongodb.domain.app.services.BookService;
import otus.springfreamwork.springdatamongodb.domain.dao.AuthorRepository;
import otus.springfreamwork.springdatamongodb.domain.dao.BookRepository;
import otus.springfreamwork.springdatamongodb.domain.dao.CommentRepository;
import otus.springfreamwork.springdatamongodb.domain.dao.GenreRepository;
import otus.springfreamwork.springdatamongodb.domain.model.Author;
import otus.springfreamwork.springdatamongodb.domain.model.Book;
import otus.springfreamwork.springdatamongodb.domain.model.Comment;
import otus.springfreamwork.springdatamongodb.domain.model.Genre;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static otus.springfreamwork.springdatamongodb.domain.model.Country.RUSSIA;

@RunWith(MockitoJUnitRunner.class)
public class BookServiceTest {

    private BookService bookService;

    @Mock
    private BookRepository bookRepository;
    @Mock
    private AuthorRepository authorRepository;
    @Mock
    private GenreRepository genreRepository;
    @Mock
    private CommentRepository commentRepository;

    @Before
    public void init() {
        bookService = new BookServiceImpl(bookRepository, authorRepository, genreRepository, commentRepository);
    }

    @Test
    public void bookServiceShouldCreateBook() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Book book = new Book("War And Piece", new Date(2018, 4, 10), parts, Collections.singleton(author), genre);
        when(genreRepository.findByName(genre.getName())).thenReturn(Optional.of(genre));
        when(authorRepository.findByNameAndSurname(author.getName(), author.getSurname())).thenReturn(Optional.of(author));
        String result = bookService.createBookByNameAndAuthorAndGenre(book.getName(), author.getName(), author.getSurname(), genre.getName());

        assertEquals("Книга успешно создана", result);
        verify(bookRepository, times(1)).save(eq(book));
        verify(bookRepository, times(1)).findByName(eq(book.getName()));
        verify(genreRepository, times(1)).findByName(eq(genre.getName()));
        verify(authorRepository, times(1)).findByNameAndSurname(eq(author.getName()), eq(author.getSurname()));
    }

    @Test
    public void bookServiceShouldCreateBookAndGenre() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Book book = new Book("War And Piece", new Date(2018, 4, 10), parts, Collections.singleton(author), genre);
        when(genreRepository.findByName(genre.getName())).thenReturn(Optional.empty());
        when(authorRepository.findByNameAndSurname(author.getName(), author.getSurname())).thenReturn(Optional.of(author));
        String result = bookService.createBookByNameAndAuthorAndGenre(book.getName(), author.getName(), author.getSurname(), genre.getName());

        assertEquals("Создан жанр\nКнига успешно создана", result);
        verify(bookRepository, times(1)).save(eq(book));
        verify(bookRepository, times(1)).findByName(eq(book.getName()));
        verify(genreRepository, times(1)).findByName(eq(genre.getName()));
        verify(authorRepository, times(1)).findByNameAndSurname(eq(author.getName()), eq(author.getSurname()));
    }

    @Test
    public void bookServiceShouldCreateBookAndAuthor() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Book book = new Book("War And Piece", new Date(2018, 4, 10), parts, Collections.singleton(author), genre);
        when(genreRepository.findByName(genre.getName())).thenReturn(Optional.of(genre));
        when(authorRepository.findByNameAndSurname(author.getName(), author.getSurname())).thenReturn(Optional.empty());
        String result = bookService.createBookByNameAndAuthorAndGenre(book.getName(), author.getName(), author.getSurname(), genre.getName());

        assertEquals("Создан автор\nКнига успешно создана", result);
        verify(bookRepository, times(1)).save(eq(book));
        verify(bookRepository, times(1)).findByName(eq(book.getName()));
        verify(genreRepository, times(1)).findByName(eq(genre.getName()));
        verify(authorRepository, times(1)).findByNameAndSurname(eq(author.getName()), eq(author.getSurname()));
    }

    @Test
    public void bookServiceShouldCreateBookAndAuthorAndGenre() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Book book = new Book("War And Piece", new Date(2018, 4, 10), parts, Collections.singleton(author), genre);
        when(genreRepository.findByName(genre.getName())).thenReturn(Optional.empty());
        when(authorRepository.findByNameAndSurname(author.getName(), author.getSurname())).thenReturn(Optional.empty());
        String result = bookService.createBookByNameAndAuthorAndGenre(book.getName(), author.getName(), author.getSurname(), genre.getName());

        assertEquals("Создан автор\nСоздан жанр\nКнига успешно создана", result);
        verify(bookRepository, times(1)).save(eq(book));
        verify(bookRepository, times(1)).findByName(eq(book.getName()));
        verify(genreRepository, times(1)).findByName(eq(genre.getName()));
        verify(authorRepository, times(1)).findByNameAndSurname(eq(author.getName()), eq(author.getSurname()));
    }

    @Test
    public void bookServiceShouldNotCreateBookCauseItsAlreadyInDB() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Book book = new Book("War And Piece", new Date(2018, 4, 10), parts, Collections.singleton(author), genre);
        when(bookRepository.findByName(eq(book.getName()))).thenReturn(Optional.of(book));
        String result = bookService.createBookByNameAndAuthorAndGenre(book.getName(), author.getName(), author.getSurname(), genre.getName());

        assertEquals("Книга уже в базе", result);
        verify(bookRepository, never()).save(eq(book));
        verify(bookRepository, times(1)).findByName(eq(book.getName()));
        verify(genreRepository, never()).findByName(eq(genre.getName()));
        verify(authorRepository, never()).findByNameAndSurname(eq(author.getName()), eq(author.getSurname()));
    }

    @Test
    public void bookServiceShouldReturnListOfBooks() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Book book = new Book("War And Piece", new Date(2018, 4, 10), parts, Collections.singleton(author), genre);
        Book book_2 = new Book("Anna Karenina", new Date(2018, 4, 10), parts, Collections.singleton(author), genre);
        List<Book> books = Arrays.asList(book, book_2);
        when(bookRepository.findAll()).thenReturn(books);
        String expected = "Список книг:\n" + book + "\n" + book_2;

        String result = bookService.getAllBooks();

        assertEquals(expected, result);
        verify(bookRepository, times(1)).findAll();
    }

    @Test
    public void bookServiceShouldReturnWarningCauseNoBooksInDB() {
        when(bookRepository.findAll()).thenReturn(Collections.emptyList());
        String expected = "Нет книг в базе";

        String result = bookService.getAllBooks();

        assertEquals(expected, result);
        verify(bookRepository, times(1)).findAll();
    }

    @Test
    public void bookServiceShouldReturnCountMessage() {
        when(bookRepository.count()).thenReturn(2L);
        String expected = "Количество книг в базе: 2";

        String result = bookService.countBooks();

        assertEquals(expected, result);
        verify(bookRepository, times(1)).count();
    }

    @Test
    public void bookRepositoryShouldReturnWarningCauseNoBookInDBForDelete() {
        when(bookRepository.findByName(anyString())).thenReturn(Optional.empty());
        String expected = "Не найдено книги в базе для удаления";

        String result = bookService.deleteBook("War And Piece");

        assertEquals(expected, result);
        verify(bookRepository, times(1)).findByName(eq("War And Piece"));
        verify(bookRepository, never()).deleteByName(anyString());
    }

    @Test
    public void bookRepositoryShouldReturnBookByNameAndSurnameMessage() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Book book = new Book("War And Piece", new Date(2018, 4, 10), parts, Collections.singleton(author), genre);
        when(bookRepository.findByName(eq(book.getName()))).thenReturn(Optional.of(book));
        String expected = "Найдена книга: " + book;

        String result = bookService.getBook(book.getName());

        assertEquals(expected, result);
        verify(bookRepository, times(1)).findByName(eq(book.getName()));
    }

    @Test
    public void bookRepositoryShouldReturnWarningCauseNoBookFound() {
        when(bookRepository.findByName(anyString())).thenReturn(Optional.empty());
        String expected = "Не найдено книги в базе";

        String result = bookService.getBook("War And Piece");

        assertEquals(expected, result);
        verify(bookRepository, times(1)).findByName(eq("War And Piece"));
    }

    @Test
    public void bookRepositoryShouldDeleteBookByNameAndSurname() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Book book = new Book("War And Piece", new Date(2018, 4, 10), parts, Collections.singleton(author), genre);
        when(bookRepository.findByName(eq(book.getName()))).thenReturn(Optional.of(book));
        String expected = "Книга успешно удалена";

        String result = bookService.deleteBook(book.getName());

        assertEquals(expected, result);
        verify(bookRepository, times(1)).findByName(eq(book.getName()));
        verify(bookRepository, times(1)).deleteByName(eq(book.getName()));
    }

    @Test
    public void bookRepositoryShouldReturnListOfBookByAuthor() {
        Author author = new Author(getRandomHexString(), "Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Book book = new Book("War And Piece", new Date(2018, 4, 10), parts, Collections.singleton(author), genre);
        when(authorRepository.findByNameAndSurname(eq(author.getName()), eq(author.getSurname()))).thenReturn(Optional.of(author));
        when(bookRepository.getByAuthorId(any())).thenReturn(Collections.singletonList(book));
        String expected = "Список книг для автора:\n" + book;

        String result = bookService.getBooksByAuthorNameAndSurname(author.getName(), author.getSurname());

        assertEquals(expected, result);
        verify(authorRepository, times(1)).findByNameAndSurname(eq(author.getName()), eq(author.getSurname()));
        verify(bookRepository, times(1)).getByAuthorId(eq(new ObjectId(author.getId())));
    }

    @Test
    public void bookRepositoryShouldReturnOnAuthorNameAndSurnameEmptyListWarning() {
        Author author = new Author(getRandomHexString(), "Leo", "Tolstoy", RUSSIA);
        when(authorRepository.findByNameAndSurname(eq(author.getName()), eq(author.getSurname()))).thenReturn(Optional.of(author));
        when(bookRepository.getByAuthorId(any())).thenReturn(Collections.emptyList());
        String expected = "Список книг для выбранного автора пуст";

        String result = bookService.getBooksByAuthorNameAndSurname(author.getName(), author.getSurname());

        assertEquals(expected, result);
        verify(authorRepository, times(1)).findByNameAndSurname(eq(author.getName()), eq(author.getSurname()));
        verify(bookRepository, times(1)).getByAuthorId(eq(new ObjectId(author.getId())));
    }

    @Test
    public void bookRepositoryShouldReturnOnAuthorNameAndSurnameWarningCauseNoAuthorInDB() {
        when(authorRepository.findByNameAndSurname(anyString(), anyString())).thenReturn(Optional.empty());
        String expected = "В базе нет такого автора";

        String result = bookService.getBooksByAuthorNameAndSurname("Leo", "Tolstoy");

        assertEquals(expected, result);
        verify(authorRepository, times(1)).findByNameAndSurname(eq("Leo"), eq("Tolstoy"));
        verify(bookRepository, never()).getByAuthorId(any());
    }

    @Test
    public void bookRepositoryShouldReturnListOfBookByGenre() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre(getRandomHexString(), "novel");
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Book book = new Book("War And Piece", new Date(2018, 4, 10), parts, Collections.singleton(author), genre);
        when(genreRepository.findByName(eq(genre.getName()))).thenReturn(Optional.of(genre));
        when(bookRepository.getByGenreId(any())).thenReturn(Collections.singletonList(book));
        String expected = "Список книг для жанра:\n" + book;

        String result = bookService.getBooksByGenreName(genre.getName());

        assertEquals(expected, result);
        verify(genreRepository, times(1)).findByName(eq(genre.getName()));
        verify(bookRepository, times(1)).getByGenreId(eq(new ObjectId(genre.getId())));
    }

    @Test
    public void bookRepositoryShouldReturnOnGenreNameEmptyListWarning() {
        Genre genre = new Genre(getRandomHexString(), "novel");
        when(genreRepository.findByName(eq(genre.getName()))).thenReturn(Optional.of(genre));
        when(bookRepository.getByGenreId(any())).thenReturn(Collections.emptyList());
        String expected = "Список книг для выбранного жанра пуст";

        String result = bookService.getBooksByGenreName(genre.getName());

        assertEquals(expected, result);
        verify(genreRepository, times(1)).findByName(eq(genre.getName()));
        verify(bookRepository, times(1)).getByGenreId(eq(new ObjectId(genre.getId())));
    }

    @Test
    public void bookRepositoryShouldReturnOnGenreNameWarningCauseNoAuthorInDB() {
        when(genreRepository.findByName(eq("novel"))).thenReturn(Optional.empty());
        String expected = "В базе нет такого жанра";

        String result = bookService.getBooksByGenreName("novel");

        assertEquals(expected, result);
        verify(genreRepository, times(1)).findByName(eq("novel"));
        verify(bookRepository, never()).getByGenreId(any());
    }

    @Test
    public void bookRepositoryShouldReturnListOfCommentsOnBook() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Genre genre = new Genre("novel");
        Map<Integer, String> parts = Collections.singletonMap(1, "partOne");
        Book book = new Book("War And Piece", new Date(2018, 4, 10), parts, Collections.singleton(author), genre);
        book.setId(getRandomHexString());
        Comment comment = new Comment("user", "so good");
        comment.setBooks(Collections.singleton(book));
        when(bookRepository.findByName(anyString())).thenReturn(Optional.of(book));
        when(commentRepository.getByBookId(any())).thenReturn(Collections.singletonList(comment));
        String expected = "Список комментариев для книги:\n" + comment;

        String result = bookService.getCommentsOnBook(book.getName());

        assertEquals(expected, result);
        verify(commentRepository, times(1)).getByBookId(eq(new ObjectId(book.getId())));
    }

    @Test
    public void bookRepositoryShouldReturnMessageOfNoCommentsOnBook() {
        when(bookRepository.findByName(anyString())).thenReturn(Optional.empty());
        String expected = "Нет комментариев на книгу";

        String result = bookService.getCommentsOnBook("War And Piece");

        assertEquals(expected, result);
        verify(bookRepository, times(1)).findByName(anyString());
        verify(commentRepository, never()).getByBookId(any());
    }

    private String getRandomHexString(){
        return new ObjectId(new Date()).toHexString();
    }

}
