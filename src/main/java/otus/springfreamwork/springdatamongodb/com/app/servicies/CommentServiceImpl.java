package otus.springfreamwork.springdatamongodb.com.app.servicies;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import otus.springfreamwork.springdatamongodb.domain.app.services.CommentService;
import otus.springfreamwork.springdatamongodb.domain.dao.BookRepository;
import otus.springfreamwork.springdatamongodb.domain.dao.CommentRepository;
import otus.springfreamwork.springdatamongodb.domain.model.Book;
import otus.springfreamwork.springdatamongodb.domain.model.Comment;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Service
public class CommentServiceImpl implements CommentService {

    private final CommentRepository commentRepository;
    private final BookRepository bookRepository;

    @Autowired
    public CommentServiceImpl(
            CommentRepository commentRepository,
            BookRepository bookRepository
    ) {
        this.commentRepository = commentRepository;
        this.bookRepository = bookRepository;
    }

    @Override
    public String createComment(String username, String commentText, String bookName) {
        Optional<Book> book = bookRepository.findByName(bookName);
        String result;
        if (!book.isPresent()) {
            result = "Не найдено книги, комментарий не создан";
        } else {
            Comment comment = new Comment(username, commentText);
            comment.setBooks(Collections.singleton(book.get()));
            commentRepository.save(comment);
            result = "Комментарий создан";
        }
        return result;
    }

    @Override
    public String getAllComments() {
        List<Comment> comments = commentRepository.findAll();
        StringBuilder stringBuilder = new StringBuilder();
        if (comments.isEmpty()) {
            stringBuilder.append("Нет комментариев в базе");
        } else {
            stringBuilder.append("Список комментариев:");
            comments.forEach(comment -> stringBuilder.append("\n").append(comment));
        }
        return stringBuilder.toString();
    }

    @Override
    public String countComments() {
        return "Количество комментариев в базе: " + commentRepository.count();
    }

    @Override
    public String deleteUsernameComments(String usernanme) {
        List<Comment> comments = commentRepository.findAllByUsername(usernanme);
        String result;
        if (comments.isEmpty()) {
            result = "Не найдено комментариев для удаления";
        } else {
            commentRepository.deleteByUsername(usernanme);
            result = "Комментарии успешно удалены";
        }
        return result;
    }

    @Override
    public String getUsernameComments(String username) {
        List<Comment> comments = commentRepository.findAllByUsername(username);
        StringBuilder stringBuilder = new StringBuilder();
        if (comments.isEmpty()) {
            stringBuilder.append("Не найдено комментариев юзера");
        } else {
            stringBuilder.append("Список комментариев юзера:");
            comments.forEach(comment -> stringBuilder.append("\n").append(comment));
        }
        return stringBuilder.toString();
    }

}
