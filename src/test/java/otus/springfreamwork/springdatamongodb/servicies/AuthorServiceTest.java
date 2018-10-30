package otus.springfreamwork.springdatamongodb.servicies;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import otus.springfreamwork.springdatamongodb.com.app.servicies.AuthorServiceImpl;
import otus.springfreamwork.springdatamongodb.domain.app.services.AuthorService;
import otus.springfreamwork.springdatamongodb.domain.dao.AuthorRepository;
import otus.springfreamwork.springdatamongodb.domain.model.Author;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static otus.springfreamwork.springdatamongodb.domain.model.Country.RUSSIA;

@RunWith(MockitoJUnitRunner.class)
public class AuthorServiceTest {

    private AuthorService authorService;

    @Mock
    private AuthorRepository authorRepository;

    @Before
    public void init() {
        authorService = new AuthorServiceImpl(authorRepository);
    }

    @Test
    public void authorServiceShouldCreateAuthorByNameAndSurname() {
        String name = "Leo";
        String surname = "Tolstoy";
        String result = authorService.createAuthorByNameAndSurname(name, surname);
        Author author = new Author(name, surname, RUSSIA);

        assertEquals("Автор успешно создан", result);
        verify(authorRepository, times(1)).save(eq(author));
        verify(authorRepository, times(1)).findByNameAndSurname(eq(name), eq(surname));
    }

    @Test
    public void authorServiceShouldNotCreateAuthorCauseItsAlreadyInDB() {
        String name = "Leo";
        String surname = "Tolstoy";
        Author author = new Author(name, surname, RUSSIA);
        when(authorRepository.findByNameAndSurname(eq(name), eq(surname))).thenReturn(Optional.of(author));
        String result = authorService.createAuthorByNameAndSurname(name, surname);

        assertEquals("Автор уже в базе", result);
        verify(authorRepository, never()).save(eq(author));
        verify(authorRepository, times(1)).findByNameAndSurname(eq(name), eq(surname));
    }

    @Test
    public void authorServiceShouldReturnListOfAuthors() {
        Author author = new Author("Leo", "Tolstoy", RUSSIA);
        Author author_2 = new Author("Fedor", "Dostoevsky", RUSSIA);
        List<Author> authors = Arrays.asList(author, author_2);
        when(authorRepository.findAll()).thenReturn(authors);
        String expected = "Список авторов:\n" + author + "\n" + author_2;

        String result = authorService.getAllAuthors();

        assertEquals(expected, result);
        verify(authorRepository, times(1)).findAll();
    }

    @Test
    public void authorServiceShouldReturnWarningCauseNoAuthorsInDB() {
        when(authorRepository.findAll()).thenReturn(Collections.emptyList());
        String expected = "Нет авторов в базе";

        String result = authorService.getAllAuthors();

        assertEquals(expected, result);
        verify(authorRepository, times(1)).findAll();
    }

    @Test
    public void authorServiceShouldReturnCountMessage() {
        when(authorRepository.count()).thenReturn(2L);
        String expected = "Количество авторов в базе: 2";

        String result = authorService.countAuthors();

        assertEquals(expected, result);
        verify(authorRepository, times(1)).count();
    }

    @Test
    public void authorRepositoryShouldReturnWarningCauseNoAuthorInDBForDelete() {
        when(authorRepository.findByNameAndSurname(anyString(), anyString())).thenReturn(Optional.empty());
        String expected = "Не найдено автора в базе для удаления";

        String result = authorService.deleteAuthor("Leo", "Tolstoy");

        assertEquals(expected, result);
        verify(authorRepository, times(1)).findByNameAndSurname(eq("Leo"), eq("Tolstoy"));
        verify(authorRepository, never()).deleteByNameAndSurname(anyString(), anyString());
    }

    @Test
    public void authorRepositoryShouldReturnAuthorByNameAndSurnameMessage() {
        String name = "Leo";
        String surname = "Tolstoy";
        Author author = new Author(name, surname, RUSSIA);
        when(authorRepository.findByNameAndSurname(eq(name), eq(surname))).thenReturn(Optional.of(author));
        String expected = "Найден автор: " + author;

        String result = authorService.getAuthor(name, surname);

        assertEquals(expected, result);
        verify(authorRepository, times(1)).findByNameAndSurname(eq(author.getName()), eq(author.getSurname()));
    }

    @Test
    public void authorRepositoryShouldReturnWarningCauseNoAuthorFound() {
        when(authorRepository.findByNameAndSurname(anyString(), anyString())).thenReturn(Optional.empty());
        String expected = "Не найдено автора в базе";

        String result = authorService.getAuthor("Leo", "Tolstoy");

        assertEquals(expected, result);
        verify(authorRepository, times(1)).findByNameAndSurname(eq("Leo"), eq("Tolstoy"));
    }

    @Test
    public void authorRepositoryShouldDeleteAuthorByNameAndSurname() {
        String name = "Leo";
        String surname = "Tolstoy";
        Author author = new Author(name, surname, RUSSIA);
        when(authorRepository.findByNameAndSurname(eq(name), eq(surname))).thenReturn(Optional.of(author));
        String expected = "Автор успешно удален";

        String result = authorService.deleteAuthor(name, surname);

        assertEquals(expected, result);
        verify(authorRepository, times(1)).findByNameAndSurname(eq(name), eq(surname));
        verify(authorRepository, times(1)).deleteByNameAndSurname(eq(name), eq(surname));
    }
}