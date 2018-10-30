package otus.springfreamwork.springdatamongodb.domain.dao;


import org.springframework.data.mongodb.repository.MongoRepository;
import otus.springfreamwork.springdatamongodb.domain.model.Genre;

import java.util.Optional;

public interface GenreRepository extends MongoRepository<Genre, String> {

    Optional<Genre> findByName(String name);

    void deleteByName(String name);
}
