package otus.springfreamwork.springdatamongodb.domain.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Objects;
import java.util.Set;

@Document
public class Comment {

    @Id
    @Indexed
    private String id;
    @Indexed
    private String username;
    private String comment;
    @DBRef
    private Set<Book> books;

    public Comment() {}

    public Comment(String id, String username, String comment) {
        this.id = id;
        this.username = username;
        this.comment = comment;
    }

    public Comment(String username, String comment) {
        this.username = username;
        this.comment = comment;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Set<Book> getBooks() {
        return books;
    }

    public void setBooks(Set<Book> books) {
        this.books = books;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Comment)) return false;
        Comment comment1 = (Comment) o;
        return Objects.equals(getUsername(), comment1.getUsername()) &&
                Objects.equals(getComment(), comment1.getComment()) &&
                Objects.equals(getBooks(), comment1.getBooks());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getUsername(), getComment(), getBooks());
    }

    @Override
    public String toString() {
        return "Comment{" +
                "id=" + id +
                ", username='" + username + '\'' +
                ", comment='" + comment + '\'' +
                ", books=" + books +
                '}';
    }
}
