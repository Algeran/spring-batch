package otus.springfreamwork.springdatamongodb.domain.dao;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import otus.springfreamwork.springdatamongodb.domain.model.Comment;

import java.util.List;

public interface CommentRepository extends MongoRepository<Comment, String> {

    List<Comment> findAllByUsername(String username);

    void deleteByUsername(String username);

    @Query("{'books.$id' : ?0 }")
    List<Comment> getByBookId(ObjectId bookId);

}
