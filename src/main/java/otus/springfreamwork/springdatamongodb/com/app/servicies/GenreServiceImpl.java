package otus.springfreamwork.springdatamongodb.com.app.servicies;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import otus.springfreamwork.springdatamongodb.domain.app.services.GenreService;
import otus.springfreamwork.springdatamongodb.domain.dao.GenreRepository;
import otus.springfreamwork.springdatamongodb.domain.model.Genre;

import java.util.List;
import java.util.Optional;

@Service
public class GenreServiceImpl implements GenreService {

    private final GenreRepository genreRepository;

    @Autowired
    public GenreServiceImpl(GenreRepository genreRepository) {
        this.genreRepository = genreRepository;
    }

    @Override
    public String createGenreByName(String name) {
        Optional<Genre> genreByName = genreRepository.findByName(name);
        String result;
        if (!genreByName.isPresent()) {
            Genre genre = new Genre(name);
            genreRepository.save(genre);
            result = "Жанр успешно создан";
        } else {
            result = "Жанр уже в базе";
        }
        return result;
    }

    @Override
    public String getAllGenres() {
        List<Genre> genres = genreRepository.findAll();
        StringBuilder stringBuilder = new StringBuilder();
        if (genres.isEmpty()) {
            stringBuilder.append("Нет жанров в базе");
        } else {
            stringBuilder.append("Список жанров:");
            genres.forEach(genre -> stringBuilder.append("\n").append(genre));
        }
        return stringBuilder.toString();
    }

    @Override
    public String countGenres() {
        return "Количество жанров в базе: " + genreRepository.count();
    }

    @Override
    public String deleteGenre(String name) {
        Genre genre = genreRepository.findByName(name).orElse(null);
        String result;
        if (genre != null) {
            genreRepository.deleteByName(name);
            result = "Жанр успешно удален";
        } else {
            result = "Не найдено жанра в базе для удаления";
        }
        return result;
    }

    @Override
    public String getGenre(String name) {
        Genre genre = genreRepository.findByName(name).orElse(null);
        String result;
        if (genre == null) {
            result = "Не найдено жанра в базе";
        } else {
            result = "Найден жанр: " + genre;
        }
        return result;
    }
}
