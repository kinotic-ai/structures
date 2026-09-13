package org.kinotic.domain.internal.api.services;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorResponse;
import co.elastic.clients.elasticsearch._types.FieldSort;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.OpType;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.mapping.DynamicMapping;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.get.GetResult;
import co.elastic.clients.elasticsearch.core.mget.MultiGetOperation;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.JsonpDeserializer;
import co.elastic.clients.json.JsonpMapperBase;
import co.elastic.clients.transport.JsonEndpoint;
import co.elastic.clients.transport.endpoints.EndpointWithResponseMapperAttr;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.commons.lang3.Validate;
import org.kinotic.core.api.crud.*;
import org.kinotic.core.api.exceptions.AlreadyExistsException;
import org.kinotic.domain.api.model.RawJson;
import org.kinotic.domain.internal.serializer.RawJsonJsonpDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.type.TypeFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by Navíd Mitchell 🤪 on 5/10/23.
 */
@Component
public class CrudServiceTemplate {

    private static final long DEFAULT_PRIORITY = 500L;

    // retry_on_conflict for partial updates. A conflict needs another write to land in the microseconds
    // between the update's internal get and its reindex, so consuming all 5 would take sustained
    // pathological contention on one document
    private static final int UPDATE_CONFLICT_RETRIES = 5;

    private static final Logger log = LoggerFactory.getLogger(CrudServiceTemplate.class);

    private final ElasticsearchAsyncClient esAsyncClient;
    private final ObjectMapper objectMapper;
    private final RawJsonJsonpDeserializer rawJsonJsonpDeserializer;

    public CrudServiceTemplate(ElasticsearchAsyncClient esAsyncClient,
                               ObjectMapper objectMapper) {
        this.esAsyncClient = esAsyncClient;
        this.objectMapper = objectMapper;
        rawJsonJsonpDeserializer = new RawJsonJsonpDeserializer(objectMapper);
    }

    /**
     * True when {@code throwable} or one of its causes is an Elasticsearch 409 — i.e. an
     * {@code op_type=create} index that hit an already-present document id.
     */
    private static boolean isVersionConflict(Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause instanceof ElasticsearchException esException && esException.status() == 409) {
                return true;
            }
        }
        return false;
    }

    /**
     * Appends a document to a data stream using Elasticsearch's {@code create} op-type. Data streams
     * are append-only — the {@code index} op and updates/deletes by id are rejected — and the document
     * id is auto-generated, so documents are retrieved by search rather than by id. The document must
     * carry a {@code @timestamp} date field.
     *
     * @param dataStreamName name of the data stream to append to
     * @param document       the document to append
     * @return a {@link Future} that will complete with the {@link IndexResponse}
     */
    public <T> Future<IndexResponse> appendToDataStream(String dataStreamName, T document) {
        return appendToDataStream(dataStreamName, document, null);
    }

    /**
     * Appends a document to a data stream using Elasticsearch's {@code create} op-type, with full
     * {@link IndexRequest} customization (e.g. {@code refresh}). Data streams are append-only and the
     * document id is auto-generated; the document must carry a {@code @timestamp} date field.
     *
     * @param dataStreamName  name of the data stream to append to
     * @param document        the document to append
     * @param builderConsumer to customize the {@link IndexRequest}, or null if no customization is needed
     * @return a {@link Future} that will complete with the {@link IndexResponse}
     */
    public <T> Future<IndexResponse> appendToDataStream(String dataStreamName,
                                                                   T document,
                                                                   Consumer<IndexRequest.Builder<T>> builderConsumer) {
        return toFuture(esAsyncClient.index((IndexRequest.Builder<T> builder) -> {
            builder.index(dataStreamName).opType(OpType.Create).document(document);
            if (builderConsumer != null) {
                builderConsumer.accept(builder);
            }
            return builder;
        }));
    }

    /**
     * Builds a bool query whose {@code filter} clauses are the supplied {@code filters}.
     * Convenience for callers that compose specialized queries from one or more term
     * filters. Requires at least one non-null filter.
     */
    public Query composeFilter(Query... filters) {
        Validate.notEmpty(filters, "filters cannot be empty");
        return Query.of(q -> q.bool(b -> {
            for (Query f : filters) if (f != null) b.filter(f);
            return b;
        }));
    }

    /**
     * Counts the number of documents in the index. Also allows for customization of the {@link CountRequest}.
     *
     * @param indexName       name of the index to count
     * @param builderConsumer to customize the {@link CountRequest}, or null if no customization is needed
     * @return a {@link Future} that will complete with the number of documents in the index
     */
    public Future<Long> count(String indexName,
                                         Consumer<CountRequest.Builder> builderConsumer) {
        return toFuture(esAsyncClient.count(builder -> {
                                                builder.index(indexName);
                                                if (builderConsumer != null) {
                                                    builderConsumer.accept(builder);
                                                }
                                                return builder;
                                            })
                                            .thenApply(CountResponse::count));
    }

    /**
     * Indexes a document only if its id is not already present, using Elasticsearch's
     * {@code create} op-type. Fails with {@link AlreadyExistsException} when a document with the
     * same id already exists, instead of overwriting it the way {@link #save} would.
     *
     * @param indexName name of the index
     * @param id        id the document must be created under
     * @param document  the document to index
     * @return a {@link Future} completing with the {@link IndexResponse}, or failing
     *         with {@link AlreadyExistsException} if the id is already taken
     */
    public <T> Future<IndexResponse> create(String indexName,
                                                       String id,
                                                       T document) {
        return create(indexName, id, document, null);
    }

    /**
     * Indexes a document only if its id is not already present, using Elasticsearch's
     * {@code create} op-type, with full {@link IndexRequest} customization. Fails with
     * {@link AlreadyExistsException} when a document with the same id already exists.
     *
     * @param indexName       name of the index
     * @param id              id the document must be created under
     * @param document        the document to index
     * @param builderConsumer to customize the {@link IndexRequest}, or null if no customization is needed
     * @return a {@link Future} completing with the {@link IndexResponse}, or failing
     *         with {@link AlreadyExistsException} if the id is already taken
     */
    public <T> Future<IndexResponse> create(String indexName,
                                                       String id,
                                                       T document,
                                                       Consumer<IndexRequest.Builder<T>> builderConsumer) {
        return toFuture(esAsyncClient.index((IndexRequest.Builder<T> builder) -> {
                    builder.index(indexName).id(id).document(document).opType(OpType.Create);
                    if (builderConsumer != null) {
                        builderConsumer.accept(builder);
                    }
                    return builder;
                }))
                .recover(throwable -> isVersionConflict(throwable)
                        ? Future.failedFuture(new AlreadyExistsException(
                                "A document with id '" + id + "' already exists in index '" + indexName + "'"))
                        : Future.failedFuture(throwable));
    }

    /**
     * Creates a data stream
     */
    public Future<Void> createDataStream(String dataStreamName) {
        return toFuture(esAsyncClient.indices().createDataStream(builder -> builder.name(dataStreamName))
                                          .thenApply(response -> null));
    }

    /**
     * Creates an index with the given name. Also allows for customization of the {@link CreateIndexRequest}.
     *
     * @param indexName       name of the index to create
     * @param failIfExists    if true will fail with an exception if the index already exists
     * @param mappings        the mappings to use for the index, or null if no mappings are needed
     * @return a {@link Future} that will complete when the index has been created
     */
    public Future<Void> createIndex(String indexName,
                                               boolean failIfExists,
                                               int numberOfShards,
                                               int numberOfReplicas,
                                               Map<String, Property> mappings) {
        return toFuture(esAsyncClient.indices().exists(builder -> builder.index(indexName))
                            .thenCompose(exists -> {
                                if (!exists.value()) {
                                    return esAsyncClient.indices()
                                                        .create(builder -> {
                                                            builder.index(indexName)
                                                                   .settings(s -> s
                                                                           .numberOfShards(String.valueOf(numberOfShards))
                                                                           .numberOfReplicas(String.valueOf(numberOfReplicas))
                                                                           .store(st -> st.type(StorageType.Fs))
                                                                   );
                                                            if (mappings != null && !mappings.isEmpty()) {
                                                                builder.mappings(m -> m
                                                                        .dynamic(DynamicMapping.Strict)
                                                                        .properties(mappings));
                                                            }
                                                            return builder;
                                                        })
                                                        .thenApply(response -> null);
                                } else {
                                    if (failIfExists) {
                                        return CompletableFuture.failedFuture(
                                                new IllegalArgumentException("Index already exists: " + indexName));
                                    } else {
                                        return CompletableFuture.completedFuture(null);
                                    }
                                }
                            }));
    }

    /**
     * Creates an index template with the given name, pattern, and mappings
     * @param templateName the name of the template
     * @param indexPattern the pattern to match the index names
     * @param dataStreamVisibility the visibility of the data stream or null if not a data stream
     * @param mappings the mappings to use for the index, or null if no mappings are needed
     * @return a {@link Future} that will complete when the index template has been created
     */
    public Future<Void> createIndexTemplate(String templateName,
                                                       String indexPattern,
                                                       DataStreamVisibility dataStreamVisibility,
                                                       int numberOfShards,
                                                       int numberOfReplicas,
                                                       Map<String, Property> mappings) {
        return createIndexTemplate(templateName, indexPattern, dataStreamVisibility, null,
                                   numberOfShards, numberOfReplicas, mappings);
    }

    /**
     * Creates an index template with the given name, pattern, and mappings, optionally configuring a
     * native data stream lifecycle retention period.
     * @param templateName the name of the template
     * @param indexPattern the pattern to match the index names
     * @param dataStreamVisibility the visibility of the data stream or null if not a data stream
     * @param dataRetention the data stream lifecycle retention period, or null for no managed lifecycle;
     *                      Elasticsearch deletes data older than this from the stream's backing indices
     * @param mappings the mappings to use for the index, or null if no mappings are needed
     * @return a {@link Future} that will complete when the index template has been created
     */
    public Future<Void> createIndexTemplate(String templateName,
                                                       String indexPattern,
                                                       DataStreamVisibility dataStreamVisibility,
                                                       Duration dataRetention,
                                                       int numberOfShards,
                                                       int numberOfReplicas,
                                                       Map<String, Property> mappings) {
        Validate.notNull(templateName, "templateName cannot be null");
        Validate.notNull(indexPattern, "indexPattern cannot be null");
        // data_retention is a data stream lifecycle concept; it has no meaning on a plain index template
        if (dataRetention != null && dataStreamVisibility == null) {
            throw new IllegalArgumentException(
                    "dataRetention can only be set for data stream templates (dataStreamVisibility must be non-null)");
        }
        return toFuture(esAsyncClient.indices().putIndexTemplate(builder -> {
            builder.name(templateName)
                   .indexPatterns(List.of(indexPattern))
                   .priority(DEFAULT_PRIORITY)
                   .create(true)
                   .template(t -> {
                                 t.settings(s -> s
                                         .numberOfShards(String.valueOf(numberOfShards))
                                         .numberOfReplicas(String.valueOf(numberOfReplicas))
                                 );
                                 if (dataRetention != null) {
                                     t.lifecycle(l -> l.dataRetention(r -> r.time(dataRetention.toSeconds() + "s")));
                                 }
                                 if(mappings != null && !mappings.isEmpty()) {
                                     t.mappings(m -> m
                                             .dynamic(DynamicMapping.Strict)
                                             .properties(mappings));
                                 }
                                 return t;
                             }
                   );
            if (dataStreamVisibility != null) {
                builder.dataStream(dataStreamVisibility);
            }
            return builder;
        }).thenApply(response -> null));
    }

    /**
     * Indexes a document only if its id is not already present, using {@link Refresh#WaitFor}
     * to guarantee read-your-write semantics for subsequent queries. Fails with
     * {@link AlreadyExistsException} when a document with the same id already exists.
     *
     * @param indexName       name of the index
     * @param id              id the document must be created under
     * @param document        the document to index
     * @param builderConsumer to customize the {@link IndexRequest}, or null if no customization is needed
     * @return a {@link Future} completing with the {@link IndexResponse} after the
     *         document is searchable, or failing with {@link AlreadyExistsException} if the id
     *         is already taken
     */
    public <T> Future<IndexResponse> createSync(String indexName,
                                                           String id,
                                                           T document,
                                                           Consumer<IndexRequest.Builder<T>> builderConsumer) {
        return create(indexName, id, document, builder -> {
            if (builderConsumer != null) {
                builderConsumer.accept(builder);
            }
            builder.refresh(Refresh.WaitFor);
        });
    }

    /**
     * Deletes a document by id. Also allows for customization of the {@link DeleteRequest}.
     *
     * @param indexName       name of the index to delete from
     * @param id              of the document to delete
     * @param builderConsumer to customize the {@link DeleteRequest}, or null if no customization is needed
     * @return a {@link Future} that will complete with the {@link DeleteResponse}
     */
    public Future<DeleteResponse> deleteById(String indexName,
                                                        String id,
                                                        Consumer<DeleteRequest.Builder> builderConsumer) {
        return toFuture(esAsyncClient.delete(builder -> {
            builder.index(indexName).id(id);
            if (builderConsumer != null) {
                builderConsumer.accept(builder);
            }
            return builder;
        }));
    }

    /**
     * Deletes a document by id using {@link Refresh#WaitFor}, guaranteeing read-your-write
     * semantics for subsequent queries. Also allows for customization of the {@link DeleteRequest}.
     *
     * @param indexName       name of the index to delete from
     * @param id              of the document to delete
     * @param builderConsumer to customize the {@link DeleteRequest}, or null if no customization is needed
     * @return a {@link Future} that will complete with the {@link DeleteResponse}
     */
    public Future<DeleteResponse> deleteByIdSync(String indexName,
                                                            String id,
                                                            Consumer<DeleteRequest.Builder> builderConsumer) {
        return deleteById(indexName, id, builder -> {
            if (builderConsumer != null) {
                builderConsumer.accept(builder);
            }
            builder.refresh(Refresh.WaitFor);
        });
    }

    /**
     * Deletes a list of documents by provided query. Also allows for customization of the {@link DeleteRequest}.
     *
     * @param indexName       name of the index to delete from
     * @param builderConsumer to customize the {@link DeleteRequest}, or null if no customization is needed
     * @return a {@link Future} that will complete with the {@link DeleteResponse}
     */
    public Future<DeleteByQueryResponse> deleteByQuery(String indexName,
                                                                  Consumer<DeleteByQueryRequest.Builder> builderConsumer) {
        return toFuture(esAsyncClient.deleteByQuery(builder -> {
            builder.index(indexName);
            if (builderConsumer != null) {
                builderConsumer.accept(builder);
            }
            return builder;
        }));
    }

    /**
     * Deletes a data stream
     */
    public Future<Void> deleteDataStream(String dataStreamName) {
        return toFuture(esAsyncClient.indices()
                                          .deleteDataStream(builder -> builder.name(dataStreamName))
                                          .thenApply(response -> null));
    }

    /**
     * Deletes an index.
     *
     * @param indexName name of the index to delete
     * @return a {@link Future} that will complete when the index has been deleted
     */
    public Future<Void> deleteIndex(String indexName) {
        return toFuture(esAsyncClient.indices()
                                          .delete(builder -> builder.index(indexName))
                                          .thenApply(response -> null));
    }

    /**
     * Deletes an index template
     */
    public Future<Void> deleteIndexTemplate(String templateName) {
        return toFuture(esAsyncClient.indices()
                                          .deleteIndexTemplate(builder -> builder.name(templateName))
                                          .thenApply(response -> null));
    }

    /** Matches documents where {@code field} is present (the inverse of {@link #missingFilter}). */
    public Query existsFilter(String field) {
        return Query.of(q -> q.exists(e -> e.field(field)));
    }

    /**
     * Finds a document by id. Also allows for customization of the {@link GetRequest}.
     *
     * @param indexName       name of the index to search
     * @param id              of the document to return
     * @param type            of the document to return
     * @param builderConsumer to customize the {@link GetRequest}, or null if no customization is needed
     * @return a {@link Future} that will complete with the document
     */
    public <T> Future<T> findById(String indexName,
                                             String id,
                                             Class<T> type,
                                             Consumer<GetRequest.Builder> builderConsumer){
        return findById(indexName, id, type, builderConsumer, null);
    }

    /**
     * Finds a document by id. Also allows for customization of the {@link GetRequest}.
     *
     * @param indexName       name of the index to search
     * @param id              of the document to return
     * @param type            of the document to return
     * @param builderConsumer to customize the {@link GetRequest}, or null if no customization is needed
     * @param resultMapper to map the {@link GetResult} to the desired type or null if the source should be returned directly
     * @return a {@link Future} that will complete with the document
     */
    public <T, R> Future<R> findById(String indexName,
                                                String id,
                                                Class<T> type,
                                                Consumer<GetRequest.Builder> builderConsumer,
                                                Function<GetResult<T>, R> resultMapper) {

        @SuppressWarnings("unchecked")
        JsonEndpoint<GetRequest, GetResponse<T>, ErrorResponse> endpoint =
            (JsonEndpoint<GetRequest, GetResponse<T>, ErrorResponse>) GetRequest._ENDPOINT;

        endpoint = new EndpointWithResponseMapperAttr<>(endpoint,
                                                        "co.elastic.clients:Deserializer:_global.get.Response.TDocument",
                                                        getDeserializer(type));

        GetRequest.Builder builder = new GetRequest.Builder();

        builder.index(indexName).id(id);
        if (builderConsumer != null) {
            builderConsumer.accept(builder);
        }

        return toFuture(esAsyncClient._transport()
                                          .performRequestAsync(builder.build(),
                                                               endpoint,
                                                               esAsyncClient._transportOptions())
                                          .thenApply(tGetResponse -> {
                                              if(resultMapper != null) {
                                                  return resultMapper.apply(tGetResponse);
                                              }else{
                                                  @SuppressWarnings("unchecked")
                                                  R result = (R)tGetResponse.source();
                                                  return result;
                                              }
                                          }));
    }

    /**
     * Issues a search with {@code size=1} and returns the single hit, or {@code null} if none.
     * Convenience for "find by unique key" lookups.
     */
    public <T> Future<T> findFirst(String indexName,
                                              Class<T> type,
                                              Consumer<SearchRequest.Builder> builderConsumer) {
        return search(indexName, Pageable.create(0, 1, Sort.unsorted()), type, builderConsumer)
                .map(page -> page.getContent().isEmpty() ? null : page.getContent().getFirst());
    }

    /** Matches documents where {@code field} is absent (the inverse of {@link #existsFilter}). */
    public Query missingFilter(String field) {
        return Query.of(q -> q.bool(b -> b.mustNot(mn -> mn.exists(e -> e.field(field)))));
    }

    /**
     * Gets multiple documents for their {@link MultiGetOperation} objects. Also allows for customization of the {@link MgetRequest}.
     * @param getOperations list of {@link MultiGetOperation} to get
     * @param type of the document to return
     * @param builderConsumer to customize the {@link MgetRequest}, or null if no customization is needed
     * @param resultMapper to map the {@link GetResult} to the desired type or null if the source should be returned directly
     * @return a {@link Future} that will complete with the documents requested
     */
    public <T, R> Future<List<R>> multiGet(List<MultiGetOperation> getOperations,
                                                      Class<T> type,
                                                      Consumer<MgetRequest.Builder> builderConsumer,
                                                      Function<GetResult<T>, R> resultMapper){
        @SuppressWarnings("unchecked")
        JsonEndpoint<MgetRequest, MgetResponse<T>, ErrorResponse> endpoint =
                (JsonEndpoint<MgetRequest, MgetResponse<T>, ErrorResponse>) MgetRequest._ENDPOINT;

        endpoint = new EndpointWithResponseMapperAttr<>(endpoint,
                                                        "co.elastic.clients:Deserializer:_global.mget.Response.TDocument",
                                                        getDeserializer(type));

        MgetRequest.Builder builder = new MgetRequest.Builder();
        builder.docs(getOperations);

        if (builderConsumer != null) {
            builderConsumer.accept(builder);
        }

        return toFuture(esAsyncClient._transport()
                                          .performRequestAsync(builder.build(),
                                                               endpoint,
                                                               esAsyncClient._transportOptions())
                                          .thenApply(response -> {

                                              List<MultiGetResponseItem<T>> recordsResponse = response.docs();
                                              ArrayList<R> content = new ArrayList<>();

                                              if(resultMapper != null) {
                                                  for (MultiGetResponseItem<T> hit : recordsResponse) {
                                                      if (hit.isResult() && hit.result().found()) {
                                                          content.add(resultMapper.apply(hit.result()));
                                                      }
                                                  }
                                              }else{
                                                  for (MultiGetResponseItem<T> hit : recordsResponse) {
                                                      if(hit.isResult() && hit.result().found()){
                                                          @SuppressWarnings("unchecked")
                                                          R result = (R)hit.result().source();
                                                          content.add(result);
                                                      }
                                                  }
                                              }
                                              return content;
                                          }));
    }

    /**
     * Applies a partial update to a document, merging only the given fields into the stored source and leaving
     * every other field untouched. With {@code upsert} true a missing document is created from the partial
     * instead of failing. Safe under concurrency: a conflicting concurrent write is retried server-side, so the
     * merge lands on the latest document version instead of failing with a version conflict.
     *
     * @param indexName name of the index containing the document
     * @param id        of the document to update
     * @param partial   the fields to merge into the document
     * @param upsert    true to create the document from {@code partial} when it does not exist
     * @return a {@link Future} that will complete when the update is applied
     */
    public Future<Void> partialUpdate(String indexName,
                                                 String id,
                                                 Map<String, Object> partial,
                                                 boolean upsert) {
        // a doc merge carries no compare-and-set semantics, so retrying re-applies the same fields
        // against the newest version and never loses a concurrent writer's fields
        return toFuture(esAsyncClient.update(u -> u.index(indexName)
                                                        .id(id)
                                                        .doc(partial)
                                                        .docAsUpsert(upsert)
                                                        .retryOnConflict(UPDATE_CONFLICT_RETRIES),
                                                  Map.class)
                                          .thenApply(response -> null));
    }

    /**
     * Merges the given fields into a document using {@link Refresh#WaitFor}, guaranteeing read-your-write
     * semantics for subsequent queries. Safe under concurrency: a conflicting concurrent write is retried
     * server-side, so the merge lands on the latest document version instead of failing with a version conflict.
     *
     * @param indexName name of the index
     * @param id        of the document to update
     * @param partial   the fields to merge into the document
     * @param upsert    true to create the document from {@code partial} when it does not exist
     * @return a {@link Future} that will complete when the update is applied
     */
    public Future<Void> partialUpdateSync(String indexName,
                                                     String id,
                                                     Map<String, Object> partial,
                                                     boolean upsert) {
        return toFuture(esAsyncClient.update(u -> u.index(indexName)
                                                        .id(id)
                                                        .doc(partial)
                                                        .docAsUpsert(upsert)
                                                        .retryOnConflict(UPDATE_CONFLICT_RETRIES)
                                                        .refresh(Refresh.WaitFor),
                                                  Map.class)
                                          .thenApply(response -> null));
    }

    /**
     * Runs a Painless script against a document using {@link Refresh#WaitFor}, guaranteeing read-your-write
     * semantics for subsequent queries. The script reads and writes the document in one shard operation, so a
     * read-modify-write expressed in it cannot lose a concurrent writer's change; a conflicting concurrent
     * write is retried server-side. A script that sets {@code ctx.op} to {@code noop} leaves the document as
     * it is and the future completes with false.
     *
     * @param indexName name of the index
     * @param id        of the document to update
     * @param source    the Painless source, reading its inputs from {@code params}
     * @param params    the values the script reads as {@code params.<name>}
     * @return a {@link Future} that will complete with true when the script changed the document and false
     * when it declined, or fail when the document does not exist
     */
    public Future<Boolean> scriptedUpdateSync(String indexName,
                                              String id,
                                              String source,
                                              Map<String, Object> params) {
        Map<String, JsonData> scriptParams = new HashMap<>();
        params.forEach((name, value) -> scriptParams.put(name, JsonData.of(value)));
        return toFuture(esAsyncClient.update(u -> u.index(indexName)
                                                        .id(id)
                                                        .script(s -> s.source(src -> src.scriptString(source))
                                                                      .params(scriptParams))
                                                        .retryOnConflict(UPDATE_CONFLICT_RETRIES)
                                                        .refresh(Refresh.WaitFor),
                                                  Map.class)
                                          .thenApply(response -> response.result() != Result.NoOp));
    }

    /**
     * Indexes a document. Also allows for customization of the {@link IndexRequest}.
     *
     * @param indexName       name of the index
     * @param id              of the document to index
     * @param document        to index
     * @param builderConsumer to customize the {@link IndexRequest}, or null if no customization is needed
     * @return a {@link Future} that will complete with the {@link IndexResponse}
     */
    public <T> Future<IndexResponse> save(String indexName,
                                                     String id,
                                                     T document,
                                                     Consumer<IndexRequest.Builder<T>> builderConsumer) {
        return toFuture(esAsyncClient.index((IndexRequest.Builder<T> builder) -> {
            builder.index(indexName).id(id).document(document);
            if (builderConsumer != null) {
                builderConsumer.accept(builder);
            }
            return builder;
        }));
    }

    /**
     * Indexes a document using {@link Refresh#WaitFor}, guaranteeing read-your-write
     * semantics for subsequent queries. Also allows for customization of the {@link IndexRequest}.
     *
     * @param indexName       name of the index
     * @param id              of the document to index
     * @param document        to index
     * @param builderConsumer to customize the {@link IndexRequest}, or null if no customization is needed
     * @return a {@link Future} that will complete with the {@link IndexResponse}
     */
    public <T> Future<IndexResponse> saveSync(String indexName,
                                                         String id,
                                                         T document,
                                                         Consumer<IndexRequest.Builder<T>> builderConsumer) {
        return save(indexName, id, document, builder -> {
            if (builderConsumer != null) {
                builderConsumer.accept(builder);
            }
            builder.refresh(Refresh.WaitFor);
        });
    }

    /**
     * Provides base functionality to get a {@link Page} of documents from elasticsearch. With the ability to customize the {@link SearchRequest}.
     * NOTE: not all customizations are supported, only the ones that make sense for a {@link Page} of documents.
     * For example aggregations are not supported.
     * This is meant to be used internally by implementors.
     *
     * @param indexName       name of the index to search
     * @param pageable        to use for the search
     * @param type            of the documents to return
     * @param builderConsumer to customize the {@link SearchRequest}, or null if no customization is needed
     * @return a {@link Future} that will complete with a {@link Page} of documents
     */
    public <T> Future<Page<T>> search(String indexName,
                                                 Pageable pageable,
                                                 Class<T> type,
                                                 Consumer<SearchRequest.Builder> builderConsumer){
        return search(indexName, pageable, type, builderConsumer, null);
    }

    /**
     * Provides base functionality to get a {@link Page} of documents from elasticsearch. With the ability to customize the {@link SearchRequest}.
     * NOTE: not all customizations are supported, only the ones that make sense for a {@link Page} of documents.
     * For example aggregations are not supported.
     * This is meant to be used internally by implementors.
     *
     * @param indexName       name of the index to search
     * @param pageable        to use for the search
     * @param type            of the documents to return
     * @param builderConsumer to customize the {@link SearchRequest}, or null if no customization is needed
     * @param hitMapper       to map the {@link Hit} to the desired type or null if the source should be returned directly
     * @return a {@link Future} that will complete with a {@link Page} of documents
     */
    public <T,R> Future<Page<R>> search(String indexName,
                                                   Pageable pageable,
                                                   Class<T> type,
                                                   Consumer<SearchRequest.Builder> builderConsumer,
                                                   Function<Hit<T>, R> hitMapper) {

        return toFuture(searchFullResponse(indexName, pageable, type, builderConsumer)
                .thenApply(response -> {

                    HitsMetadata<T> hitsMetadata = response.hits();
                    List<R> content = new ArrayList<>(hitsMetadata.hits().size());
                    List<FieldValue> lastSort = null;

                    if(hitMapper != null) {
                        for (Hit<T> hit : hitsMetadata.hits()) {
                            content.add(hitMapper.apply(hit));
                            lastSort = hit.sort();
                        }
                    }else {
                        for (Hit<T> hit : hitsMetadata.hits()) {
                            @SuppressWarnings("unchecked")
                            R result = (R)hit.source();
                            content.add(result);
                            lastSort = hit.sort();
                        }
                    }

                    if(pageable instanceof CursorPageable) {
                        String cursor = null;
                        if (lastSort != null) {
                            try {
                                cursor = objectMapper.writeValueAsString(lastSort);
                            } catch(JacksonException e){
                                throw new IllegalStateException("Sort Array could not be serialized to JSON", e);
                            }
                        }
                        return new CursorPage<>(content,
                                                cursor,
                                                null);
                    }else{
                        return new Page<>(content,
                                          Objects.requireNonNull(hitsMetadata.total(),
                                                                 "System Error total hits not available")
                                                 .value());
                    }
                }));
    }

    public Future<Void> syncIndex(String indexName) {
        return toFuture(esAsyncClient.indices()
                                     .refresh(b -> b.index(indexName)))
                .mapEmpty();
    }

    /** Builds a {@code term} query for {@code field} equal to {@code value}. */
    public Query termFilter(String field, String value) {
        return TermQuery.of(t -> t.field(field).value(value))._toQuery();
    }

    /** Builds a {@code term} query for {@code field} equal to {@code value}. */
    public Query termFilter(String field, boolean value) {
        return TermQuery.of(t -> t.field(field).value(value))._toQuery();
    }

    /** Builds a {@code term} query for {@code field} equal to {@code value}. */
    public Query termFilter(String field, long value) {
        return TermQuery.of(t -> t.field(field).value(value))._toQuery();
    }

    /** Builds a {@code term} query for {@code field} equal to {@code value}. */
    public Query termFilter(String field, double value) {
        return TermQuery.of(t -> t.field(field).value(value))._toQuery();
    }

    public Future<Void> updateIndexMapping(String indexName,
                                                      Map<String, Property> mappings) {
        return toFuture(esAsyncClient.indices().exists(builder -> builder.index(indexName))
                            .thenCompose(exists -> {
                                if (exists.value()) {
                                    return esAsyncClient.indices()
                                                        .putMapping(builder -> {
                                                            builder.index(indexName);
                                                            if (mappings != null && !mappings.isEmpty()) {
                                                                builder.dynamic(DynamicMapping.Strict)
                                                                       .properties(mappings);
                                                            }
                                                            return builder;
                                                        })
                                                        .thenApply(response -> null);
                                } else {
                                    return CompletableFuture.failedFuture(
                                            new IllegalArgumentException("Index " + indexName + " does not exist"));
                                }
                            }));
    }

    /**
     * Updates an existing index template
     */
    public Future<Void> updateIndexTemplate(String templateName,
                                                       Map<String, Property> mappings) {
        Validate.notNull(templateName, "templateName cannot be null");
        Validate.notNull(mappings, "mappings cannot be null");
        Validate.notEmpty(mappings, "mappings cannot be empty");

        return toFuture(esAsyncClient.indices()
                            .existsIndexTemplate(builder -> builder.name(templateName))
                            .thenCompose(exists -> {
                                if (!exists.value()) {
                                    return CompletableFuture.failedFuture(
                                            new IllegalArgumentException("Index template " + templateName + " does not exist"));
                                }

                                // Fetch the existing template
                                return esAsyncClient.indices()
                                                    .getIndexTemplate(builder -> builder.name(templateName))
                                                    .thenCompose(response -> {
                                                        IndexTemplateWithRollover existingTemplate = response.indexTemplates().getFirst()
                                                                                                             .indexTemplate();

                                                        if (existingTemplate == null) {
                                                            return CompletableFuture.failedFuture(
                                                                    new IllegalStateException("Failed to retrieve template " + templateName));
                                                        }

                                                        // Update the template with existing settings and patterns
                                                        return esAsyncClient.indices()
                                                                            .putIndexTemplate(builder -> {
                                                                                builder.name(templateName)
                                                                                       .indexPatterns(existingTemplate.indexPatterns())
                                                                                       .priority(Objects.requireNonNullElse(existingTemplate.priority(), DEFAULT_PRIORITY));

                                                                                // Preserve data stream configuration if present
                                                                                if (existingTemplate.dataStream() != null) {
                                                                                    builder.dataStream(d -> d);
                                                                                }

                                                                                // Apply existing settings and new mappings
                                                                                builder.template(t -> {
                                                                                    IndexTemplateSummaryWithRollover template = existingTemplate.template();
                                                                                    if (template != null && template.settings() != null) {
                                                                                        t.settings(template.settings());
                                                                                    }
                                                                                    t.mappings(m -> m
                                                                                            .dynamic(DynamicMapping.Strict)
                                                                                            .properties(mappings));
                                                                                    return t;
                                                                                });

                                                                                return builder;
                                                                            })
                                                                            .thenApply(pr -> null);
                                                    });
                            }));
    }

    /**
     * Verifies that the given Elasticsearch index exists, throwing {@link IllegalStateException}
     * if it does not. This is intended to be called from {@code @PostConstruct} methods on
     * services that depend on a pre-existing index. A missing index typically indicates that
     * the expected migration has not been applied.
     *
     * @param indexName name of the index to check
     * @throws IllegalStateException if the index does not exist or if the existence check fails
     */
    public void verifyIndexExists(String indexName) {
        try {
            boolean exists = esAsyncClient.indices()
                                          .exists(b -> b.index(indexName))
                                          .get()
                                          .value();
            if (!exists) {
                throw new IllegalStateException(
                        "Elasticsearch index '" + indexName + "' does not exist. "
                        + "Did you forget to add a migration in kinotic-migration/src/main/resources/migrations/?");
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to verify existence of index '" + indexName + "'", e);
        }
    }

    /**
     * Converts a {@link CompletableFuture} produced by an asynchronous client (the Elasticsearch
     * client, a Caffeine loader) into a {@link Future} whose handlers are dispatched on the Vert.x
     * context that is current at the moment this method is invoked. Any {@code compose} /
     * {@code map} / {@code onComplete} attached by the caller will then run on that context, which
     * means {@code Vertx.currentContext()} — and by extension
     * {@link org.kinotic.core.api.security.SecurityContext#currentParticipant()} —
     * will be observable across the client's async boundary. Failures are delivered as the
     * raw cause, never wrapped in {@link CompletionException}.
     * <p>
     * When invoked outside of any Vert.x context, handlers run on the completing thread so
     * non-Vert.x callers still work.
     */
    public <T> Future<T> toFuture(CompletableFuture<T> es) {
        // a failure that crossed a dependent stage arrives CompletionException-wrapped; strip it here
        // so every Vert.x consumer sees the raw cause. A bare Promise.promise() would not re-dispatch
        // onto the context, so the context-bound fromCompletionStage overload is required.
        CompletableFuture<T> unwrapped = new CompletableFuture<>();
        es.whenComplete((result, err) -> {
            if (err != null) {
                unwrapped.completeExceptionally(err instanceof CompletionException && err.getCause() != null
                                                        ? err.getCause() : err);
            } else {
                unwrapped.complete(result);
            }
        });
        Context ctx = Vertx.currentContext();
        Future<T> ret;
        if (ctx != null) {
            ret = Future.fromCompletionStage(unwrapped, ctx);
        } else {
            ret = Future.fromCompletionStage(unwrapped);
        }
        return ret;
    }

    private <T> JsonpDeserializer<T> getDeserializer(Class<T> type) {
        if (RawJson.class.isAssignableFrom(type)) {
            @SuppressWarnings("unchecked")
            JsonpDeserializer<T> deserializer = (JsonpDeserializer<T>) rawJsonJsonpDeserializer;
            return deserializer;
        }

        // Try the built-in deserializers first to avoid repeated lookups in the Jsonp mapper for client-defined classes
        JsonpDeserializer<T> result = JsonpMapperBase.findDeserializer(type);
        if (result != null) {
            return result;
        }

        return JsonpDeserializer.of(type);
    }

    /**
     * Provides base functionality to get a {@link Page} of documents from elasticsearch. With the ability to customize the {@link SearchRequest}.
     *
     * @param indexName       name of the index to search
     * @param pageable        to use for the search
     * @param type            of the documents to return
     * @param builderConsumer to customize the {@link SearchRequest}, or null if no customization is needed
     * @return a {@link Future} that will complete with a {@link SearchResponse} of documents
     */
    private <T> CompletableFuture<SearchResponse<T>> searchFullResponse(String indexName,
                                                                        Pageable pageable,
                                                                        Class<T> type,
                                                                        Consumer<SearchRequest.Builder> builderConsumer) {

        Validate.notNull(indexName, "indexName cannot be null");
        Validate.notNull(pageable, "pageable cannot be null");

        @SuppressWarnings("unchecked")
        JsonEndpoint<SearchRequest, SearchResponse<T>, ErrorResponse> endpoint =
                (JsonEndpoint<SearchRequest, SearchResponse<T>, ErrorResponse>) SearchRequest._ENDPOINT;
        endpoint = new EndpointWithResponseMapperAttr<>(endpoint,
                                                        "co.elastic.clients:Deserializer:_global.search.Response.TDocument",
                                                        getDeserializer(type));

        SearchRequest.Builder builder = new SearchRequest.Builder();

        builder.index(indexName)
               .size(pageable.getPageSize());

        if(pageable instanceof OffsetPageable){

            builder.from(((OffsetPageable)pageable).getPageNumber() * pageable.getPageSize())
                   .trackTotalHits(t -> t.enabled(true));

        } else if (pageable instanceof CursorPageable cursorPageable){

            try {
                if(pageable.getSort() == null || pageable.getSort().isUnsorted()){
                    throw new IllegalArgumentException("When using Cursor based paging you MUST provide a Sort value.");
                }

                String cursorJson = cursorPageable.getCursor();
                // this can be null or empty to indicate the first page
                if(cursorJson != null && !cursorJson.isEmpty()) {
                    TypeFactory typeFactory = objectMapper.getTypeFactory();
                    List<FieldValue> searchAfter = objectMapper.readValue(cursorJson,
                                                                          typeFactory.constructCollectionType(List.class,
                                                                                                              FieldValue.class));
                    builder.searchAfter(searchAfter);
                }
            } catch (JacksonException e) {
                throw new IllegalStateException("Cursor could not be deserialized", e);
            }

        } else {
            throw new IllegalArgumentException("Unsupported Pageable type: "+pageable.getClass().getName());
        }

        if(pageable.getSort() != null) {
            for (Order order : pageable.getSort()) {
                builder.sort(s -> s.field(f -> {
                    String property = order.getProperty();
                    FieldSort.Builder fieldSortBuilder
                            = f.field(property)
                               .order(order.isAscending() ? SortOrder.Asc : SortOrder.Desc);

                    // This is a nested sort, so we must set an additional field
                    // TODO: This must only be applied if the property is a nested field, meaning it has the nested annotation.
//                    if (property.contains(".")) {
//                        String baseField = property.substring(0, property.lastIndexOf("."));
//                        fieldSortBuilder.nested(n -> n.path(baseField));
//                    }

                    return fieldSortBuilder;
                }));
            }
        }

        if (builderConsumer != null) {
            builderConsumer.accept(builder);
        }

        SearchRequest request = builder.build();

        if(log.isTraceEnabled()) {
            // wrapped, so toString() will not be called if trace is not enabled
          //  log.trace("Query: \n {}", request.toString());
        }

        return esAsyncClient._transport()
                            .performRequestAsync(request, endpoint, esAsyncClient._transportOptions());
    }

}
