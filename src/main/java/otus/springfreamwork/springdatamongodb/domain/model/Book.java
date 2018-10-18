package otus.springfreamwork.springdatamongodb.domain.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Calendar.DAY_OF_YEAR;
import static java.util.Calendar.YEAR;

@Document
public class Book{

    @Id
    @Indexed
    private String id;
    @Indexed(unique = true)
    private String name;
    private Date publishedDate;
    @Transient
    private int ageYears;
    private Map<Integer,String> parts;
    @DBRef
    private Set<Author> authors;
    @DBRef
    private Genre genre;

    public Book() { }

    @PersistenceConstructor
    public Book(String name, Date publishedDate, Map<Integer, String> parts, Set<Author> authors, Genre genre) {
        setName(name);
        this.publishedDate = publishedDate;
        this.parts = parts;
        this.authors = authors;
        this.genre = genre;

        if (this.publishedDate == null) {
            ageYears = -1;
        } else {
            Calendar published = Calendar.getInstance();
            published.setTime(this.publishedDate);
            Calendar now = Calendar.getInstance();
            int adjust = 0;
            if (now.get(DAY_OF_YEAR) - published.get(DAY_OF_YEAR) < 0) {
                adjust = -1;
            }
            ageYears = now.get(YEAR) - published.get(YEAR) + adjust;
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getPublishedDate() {
        return publishedDate;
    }

    public void setPublishedDate(Date publishedDate) {
        this.publishedDate = publishedDate;
    }

    public void calculateAge() {
        if (this.publishedDate == null) {
            ageYears = -1;
        } else {
            Calendar published = Calendar.getInstance();
            published.setTime(this.publishedDate);
            Calendar now = Calendar.getInstance();
            int adjust = 0;
            if (now.get(DAY_OF_YEAR) - published.get(DAY_OF_YEAR) < 0) {
                adjust = -1;
            }
            ageYears = now.get(YEAR) - published.get(YEAR) + adjust;
        }
    }

    public int getAgeYears() {
        return ageYears;
    }

    public void setAgeYears(int ageYears) {
        this.ageYears = ageYears;
    }

    public Map<Integer, String> getParts() {
        return parts;
    }

    public void setParts(Map<Integer, String> parts) {
        this.parts = parts;
    }

    public Set<Author> getAuthors() {
        return authors;
    }

    public void setAuthors(Set<Author> authors) {
        this.authors = authors;
    }

    public Genre getGenre() {
        return genre;
    }

    public void setGenre(Genre genre) {
        this.genre = genre;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Book)) return false;
        Book book = (Book) o;
        return getAgeYears() == book.getAgeYears() &&
                Objects.equals(getPublishedDate(), book.getPublishedDate()) &&
                Objects.equals(getParts(), book.getParts()) &&
                Objects.equals(getAuthors(), book.getAuthors()) &&
                Objects.equals(getGenre(), book.getGenre());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getPublishedDate(), getAgeYears(), getParts(), getAuthors(), getGenre());
    }

    @Override
    public String toString() {
        return "Book{" +
                "id=" + getId() +
                ", name=" + getName() +
                ", publishedDate=" + publishedDate +
                ", ageYears=" + ageYears +
                ", parts=" + parts +
                ", author=" + authors +
                ", genre=" + genre +
                '}';
    }
}
