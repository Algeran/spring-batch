package otus.springfreamwork.springdatamongodb.domain.app.services;

public interface AuthorService {

    String createAuthorByNameAndSurname(String name, String surname);

    String getAllAuthors();

    String countAuthors();

    String getAuthor(String name, String surname);

    String deleteAuthor(String name, String surname);
}
