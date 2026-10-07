package org.presight.ai.assistant.memory;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.presight.ai.common.id.UuidV7;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
class MongoConversationStore implements ConversationStore, ApplicationRunner {

    @Document("conversations")
    record ConversationDoc(@Id String id, String userId, String title, Set<String> issuedHolds, long nextSeq,
                           Instant createdAt, Instant updatedAt) {

        Conversation toConversation() {
            return new Conversation(id, userId, title,
                    issuedHolds == null ? Set.of() : issuedHolds.stream().map(UUID::fromString).collect(java.util.stream.Collectors.toSet()),
                    nextSeq, createdAt, updatedAt);
        }
    }

    @Document("messages")
    record MessageDoc(@Id String id, String conversationId, long seq, Role role, String content, List<String> toolCalls,
                      Integer tokensIn, Integer tokensOut, Long latencyMs, Instant createdAt) {

        Message toMessage() {
            return new Message(id, conversationId, seq, role, content, toolCalls, tokensIn, tokensOut, latencyMs, createdAt);
        }
    }

    private final ReactiveMongoTemplate mongo;

    MongoConversationStore(ReactiveMongoTemplate mongo) {
        this.mongo = mongo;
    }

    @Override
    public Mono<Conversation> create(String userId, String title) {
        Instant now = Instant.now();
        return mongo.insert(new ConversationDoc(UuidV7.generate().toString(), userId, title, Set.of(), 0, now, now))
                .map(ConversationDoc::toConversation);
    }

    @Override
    public Mono<Conversation> findOwned(String conversationId, String userId) {
        return mongo.findOne(Query.query(Criteria.where("_id").is(conversationId).and("userId").is(userId)), ConversationDoc.class)
                .map(ConversationDoc::toConversation);
    }

    @Override
    public Flux<Conversation> listOwned(String userId, int limit) {
        return mongo.find(Query.query(Criteria.where("userId").is(userId))
                        .with(Sort.by(Sort.Direction.DESC, "updatedAt")).limit(limit), ConversationDoc.class)
                .map(ConversationDoc::toConversation);
    }

    @Override
    public Flux<Message> recent(String conversationId, int turns) {
        return mongo.find(Query.query(Criteria.where("conversationId").is(conversationId))
                        .with(Sort.by(Sort.Direction.DESC, "seq")).limit(turns * 2), MessageDoc.class)
                .map(MessageDoc::toMessage)
                .collectList()
                .flatMapMany(list -> Flux.fromIterable(list.reversed()));
    }

    @Override
    public Flux<Message> all(String conversationId) {
        return mongo.find(Query.query(Criteria.where("conversationId").is(conversationId)).with(Sort.by("seq")), MessageDoc.class)
                .map(MessageDoc::toMessage);
    }

    @Override
    public Mono<Long> reserveTurn(String conversationId) {
        return mongo.findAndModify(Query.query(Criteria.where("_id").is(conversationId)),
                        new Update().inc("nextSeq", 2).set("updatedAt", Instant.now()),
                        FindAndModifyOptions.options().returnNew(false), ConversationDoc.class)
                .map(ConversationDoc::nextSeq);
    }

    @Override
    public Mono<Void> append(Message m) {
        return mongo.insert(new MessageDoc(m.id(), m.conversationId(), m.seq(), m.role(), m.content(), m.toolCalls(),
                m.tokensIn(), m.tokensOut(), m.latencyMs(), m.createdAt())).then();
    }

    @Override
    public Mono<Void> recordIssuedHolds(String conversationId, Set<UUID> holdIds) {
        if (holdIds.isEmpty()) {
            return Mono.empty();
        }
        return mongo.updateFirst(Query.query(Criteria.where("_id").is(conversationId)),
                new Update().addToSet("issuedHolds").each(holdIds.stream().map(UUID::toString).toArray()),
                ConversationDoc.class).then();
    }

    @Override
    public void run(ApplicationArguments args) {
        mongo.indexOps(MessageDoc.class)
                .createIndex(new Index().on("conversationId", Sort.Direction.ASC).on("seq", Sort.Direction.ASC).unique())
                .then(mongo.indexOps(ConversationDoc.class)
                        .createIndex(new Index().on("userId", Sort.Direction.ASC).on("updatedAt", Sort.Direction.DESC)))
                .block();
    }
}
