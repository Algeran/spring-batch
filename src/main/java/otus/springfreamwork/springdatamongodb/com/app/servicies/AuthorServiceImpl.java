package otus.springfreamwork.springdatamongodb.com.app.servicies;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import otus.springfreamwork.springdatamongodb.domain.app.services.AuthorService;
import otus.springfreamwork.springdatamongodb.domain.dao.AuthorRepository;
import otus.springfreamwork.springdatamongodb.domain.model.Author;

import java.util.List;
import java.util.Optional;

import static otus.springfreamwork.springdatamongodb.domain.model.Country.RUSSIA;


@Service
public class AuthorServiceImpl implements AuthorService {

    private final AuthorRepository authorRepository;

    @Autowired
    public AuthorServiceImpl(AuthorRepository authorRepository) {
        this.authorRepository = authorRepository;
    }

    @Override
    public String createAuthorByNameAndSurname(String name, String surname) {
        Optional<Author> author = authorRepository.findByNameAndSurname(name, surname);
        String result;
        if (!author.isPresent()) {
            authorRepository.save(new Author(name, surname, RUSSIA));
            result = "Автор успешно создан";
        } else {
            result = "Автор уже в базе";
        }
        return result;
    }

    @Override
    public String getAllAuthors() {
        List<Author> authors = authorRepository.findAll();
        StringBuilder stringBuilder = new StringBuilder();
        if (authors.isEmpty()) {
            stringBuilder.append("Нет авторов в базе");
        } else {
            stringBuilder.append("Список авторов:");
            authors.forEach(author -> stringBuilder.append("\n").append(author));
        }
        return stringBuilder.toString();
    }

    @Override
    public String countAuthors() {
        return "Количество авторов в базе: " + authorRepository.count();
    }

    @Override
    public String getAuthor(String name, String surname) {
        Author author = authorRepository.findByNameAndSurname(name, surname).orElse(null);
        String result;
        if (author == null) {
            result = "Не найдено автора в базе";
        } else {
            result = "Найден автор: " + author;
        }
        return result;
    }

    @Override
    public String deleteAuthor(String name, String surname) {
        Author author = authorRepository.findByNameAndSurname(name, surname).orElse(null);
        String result;
        if (author != null) {
            authorRepository.deleteByNameAndSurname(name, surname);
            result = "Автор успешно удален";
        } else {
            result = "Не найдено автора в базе для удаления";
        }
        return result;
    }
}
