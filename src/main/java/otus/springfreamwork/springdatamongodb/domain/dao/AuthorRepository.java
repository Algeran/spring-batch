package otus.springfreamwork.springdatamongodb.domain.dao;

import org.springframework.data.mongodb.repository.MongoRepository;
import otus.springfreamwork.springdatamongodb.domain.model.Author;

import java.util.Optional;

public interface AuthorRepository extends MongoRepository<Author, String> {

    Optional<Author> findByNameAndSurname(String name, String surname);

    void deleteByNameAndSurname(String name, String surname);
}
