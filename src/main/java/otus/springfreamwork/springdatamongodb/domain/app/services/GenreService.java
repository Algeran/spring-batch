package otus.springfreamwork.springdatamongodb.domain.app.services;

public interface GenreService {

    String createGenreByName(String name);

    String getAllGenres();

    String countGenres();

    String deleteGenre(String name);

    String getGenre(String name);
}
