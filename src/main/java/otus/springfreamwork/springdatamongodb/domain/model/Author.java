package otus.springfreamwork.springdatamongodb.domain.model;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Objects;

@Document
@CompoundIndex(name = "name_surname", def = "{'name':1, 'surname':1}", unique = true)
public class Author {

    @Id
    @Indexed
    private String id;
    private String name;
    private String surname;
    private Country country;

    public Author() {}

    public Author(String name, String surname, Country country) {
        setName(name);
        this.surname = surname;
        this.country = country;
    }

    @PersistenceConstructor
    public Author(String id, String name, String surname, @Value("#root.country ?: 'NONE'") Country country) {
        this(name, surname, country);
        setId(id);
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

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public Country getCountry() {
        return country;
    }

    public void setCountry(Country country) {
        this.country = country;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Author author = (Author) o;
        return Objects.equals(getName(), author.getName()) &&
                Objects.equals(surname, author.surname) &&
                country == author.country;
    }

    @Override
    public int hashCode() {

        return Objects.hash(getName(), surname, country);
    }

    @Override
    public String toString() {
        return "Author{" +
                "id=" + getId() +
                ", name='" + getName() + '\'' +
                ", surname='" + surname + '\'' +
                ", country=" + country +
                '}';
    }
}
