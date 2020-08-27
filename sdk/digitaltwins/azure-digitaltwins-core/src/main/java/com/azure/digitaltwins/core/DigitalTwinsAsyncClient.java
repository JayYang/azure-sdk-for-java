// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.digitaltwins.core;

import com.azure.core.annotation.ReturnType;
import com.azure.core.annotation.ServiceClient;
import com.azure.core.annotation.ServiceMethod;
import com.azure.core.http.HttpPipeline;
import com.azure.core.http.rest.PagedFlux;
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.http.rest.PagedResponseBase;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.core.util.logging.ClientLogger;
import com.azure.core.util.serializer.JacksonAdapter;
import com.azure.digitaltwins.core.implementation.AzureDigitalTwinsAPIImpl;
import com.azure.digitaltwins.core.implementation.AzureDigitalTwinsAPIImplBuilder;
import com.azure.digitaltwins.core.implementation.models.IncomingRelationship;
import com.azure.digitaltwins.core.implementation.serializer.DigitalTwinsStringSerializer;
import com.azure.digitaltwins.core.util.DigitalTwinsResponse;
import com.azure.digitaltwins.core.util.DigitalTwinsResponseHeaders;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.azure.core.util.FluxUtil.withContext;


/**
 * This class provides a client for interacting asynchronously with an Azure Digital Twins instance.
 *
 * <p>
 * This client is instantiated through {@link DigitalTwinsClientBuilder}.
 * </p>
 *
 * <p>
 * This client allows for management of digital twins, their components, and their relationships. It also allows for managing
 * the digital twin models and event routes tied to your Azure Digital Twins instance.
 * </p>
 */
@ServiceClient(builder = DigitalTwinsClientBuilder.class)
public final class DigitalTwinsAsyncClient {
    private static final ClientLogger logger = new ClientLogger(DigitalTwinsAsyncClient.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private final DigitalTwinsServiceVersion serviceVersion;
    private final AzureDigitalTwinsAPIImpl protocolLayer;

    DigitalTwinsAsyncClient(HttpPipeline pipeline, DigitalTwinsServiceVersion serviceVersion, String host) {
        final SimpleModule stringModule = new SimpleModule("String Serializer");
        stringModule.addSerializer(new DigitalTwinsStringSerializer(String.class, mapper));

        JacksonAdapter jacksonAdapter = new JacksonAdapter();
        jacksonAdapter.serializer().registerModule(stringModule);

        this.protocolLayer = new AzureDigitalTwinsAPIImplBuilder()
            .host(host)
            .pipeline(pipeline)
            .serializerAdapter(jacksonAdapter)
            .buildClient();
        this.serviceVersion = serviceVersion;
    }

    /**
     * Gets the Azure Digital Twins service API version that this client is configured to use for all service requests.
     * Unless configured while building this client through {@link DigitalTwinsClientBuilder#serviceVersion(DigitalTwinsServiceVersion)},
     * this value will be equal to the latest service API version supported by this client.
     *
     * @return The Azure Digital Twins service API version.
     */
    public DigitalTwinsServiceVersion getServiceVersion() {
        return this.serviceVersion;
    }

    /**
     * Gets the {@link HttpPipeline} that this client is configured to use for all service requests. This pipeline can
     * be customized while building this client through {@link DigitalTwinsClientBuilder#httpPipeline(HttpPipeline)}.
     *
     * @return The {@link HttpPipeline} that this client uses for all service requests.
     */
    public HttpPipeline getHttpPipeline() {
        return this.protocolLayer.getHttpPipeline();
    }

    // TODO: This is a temporary implementation for sample purposes. This should be spruced up/replaced once this API is actually designed.
    // Input is String and output is Response<String>.
    @ServiceMethod(returns = ReturnType.SINGLE)
    public Mono<DigitalTwinsResponse<String>> createDigitalTwinWithResponse(String digitalTwinId, String digitalTwin) {
        return withContext(context -> createDigitalTwinWithResponse(digitalTwinId, digitalTwin, context));
    }

    Mono<DigitalTwinsResponse<String>> createDigitalTwinWithResponse(String digitalTwinId, String digitalTwin, Context context) {
        return protocolLayer
            .getDigitalTwins()
            .addWithResponseAsync(digitalTwinId, digitalTwin, context)
            .flatMap(response -> {
                try {
                    String jsonResponse = mapper.writeValueAsString(response.getValue());
                    DigitalTwinsResponseHeaders twinHeaders = mapper.convertValue(response.getDeserializedHeaders(), DigitalTwinsResponseHeaders.class);
                    return Mono.just(new DigitalTwinsResponse<>(response.getRequest(), response.getStatusCode(), response.getHeaders(), jsonResponse, twinHeaders));
                } catch (JsonProcessingException e) {
                    return Mono.error(e);
                }
            });
    }

    // TODO: This is a temporary implementation for sample purposes. This should be spruced up/replaced once this API is actually designed.
    // Input is Object and output is Response<T>.
    @ServiceMethod(returns = ReturnType.SINGLE)
    public <T> Mono<DigitalTwinsResponse<T>> createDigitalTwinWithResponse(String digitalTwinId, Object digitalTwin, Class<T> clazz) {
        return withContext(context -> createDigitalTwinWithResponse(digitalTwinId, digitalTwin, clazz, context));
    }

    <T> Mono<DigitalTwinsResponse<T>> createDigitalTwinWithResponse(String digitalTwinId, Object digitalTwin, Class<T> clazz, Context context) {
        return protocolLayer
            .getDigitalTwins()
            .addWithResponseAsync(digitalTwinId, digitalTwin, context)
            .map(response -> {
                T genericResponse = mapper.convertValue(response.getValue(), clazz);
                DigitalTwinsResponseHeaders twinHeaders = mapper.convertValue(response.getDeserializedHeaders(), DigitalTwinsResponseHeaders.class);
                return new DigitalTwinsResponse<>(response.getRequest(), response.getStatusCode(), response.getHeaders(), genericResponse, twinHeaders);
            });
    }

    /**
     * Creates a relationship on a digital twin.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @param relationshipId The Id of the relationship to be created.
     * @param relationship The application/json relationship to be created.
     * @return The application/json relationship created.
     */
    @ServiceMethod(returns = ReturnType.SINGLE)
    public Mono<String> createRelationship(String digitalTwinId, String relationshipId, String relationship) {
        return withContext(context -> createRelationship(digitalTwinId, relationshipId, relationship, context))
            .map(DigitalTwinsResponse::getValue);
    }

    /**
     * Creates a relationship on a digital twin.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @param relationshipId The Id of the relationship to be created.
     * @param relationship The application/json relationship to be created.
     * @return A REST response containing the application/json relationship created.
     */
    @ServiceMethod(returns = ReturnType.SINGLE)
    public Mono<DigitalTwinsResponse<String>> createRelationshipWithResponse(String digitalTwinId, String relationshipId, String relationship) {
        return withContext(context -> createRelationship(digitalTwinId, relationshipId, relationship, context));
    }

    Mono<DigitalTwinsResponse<String>> createRelationship(String digitalTwinId, String relationshipId, String relationship, Context context) {
        return protocolLayer
            .getDigitalTwins()
            .addRelationshipWithResponseAsync(digitalTwinId, relationshipId, relationship, context)
            .flatMap(response -> {
                try {
                    String jsonResponse = mapper.writeValueAsString(response.getValue());
                    DigitalTwinsResponseHeaders twinHeaders = mapper.convertValue(response.getDeserializedHeaders(), DigitalTwinsResponseHeaders.class);
                    return Mono.just(new DigitalTwinsResponse<>(response.getRequest(), response.getStatusCode(), response.getHeaders(), jsonResponse, twinHeaders));
                } catch (JsonProcessingException e) {
                    return Mono.error(e);
                }
            });
    }

    /**
     * Creates a relationship on a digital twin.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @param relationshipId The Id of the relationship to be created.
     * @param relationship The relationship to be created.
     * @param clazz The model class to convert the relationship to.
     * @param <T> The generic type to convert the relationship to.
     * @return The relationship created.
     */
    @ServiceMethod(returns = ReturnType.SINGLE)
    public <T> Mono<T> createRelationship(String digitalTwinId, String relationshipId, Object relationship, Class<T> clazz) {
        return withContext(context -> createRelationship(digitalTwinId, relationshipId, relationship, clazz, context))
            .map(DigitalTwinsResponse::getValue);
    }

    /**
     * Creates a relationship on a digital twin.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @param relationshipId The Id of the relationship to be created.
     * @param relationship The relationship to be created.
     * @param clazz The model class to convert the relationship to.
     * @param <T> The generic type to convert the relationship to.
     * @return A REST response containing the relationship created.
     */
    @ServiceMethod(returns = ReturnType.SINGLE)
    public <T> Mono<DigitalTwinsResponse<T>> createRelationshipWithResponse(String digitalTwinId, String relationshipId, Object relationship, Class<T> clazz) {
        return withContext(context -> createRelationship(digitalTwinId, relationshipId, relationship, clazz, context));
    }

    <T> Mono<DigitalTwinsResponse<T>> createRelationship(String digitalTwinId, String relationshipId, Object relationship, Class<T> clazz, Context context) {
        return protocolLayer
            .getDigitalTwins()
            .addRelationshipWithResponseAsync(digitalTwinId, relationshipId, relationship, context)
            .map(response -> {
                T genericResponse = mapper.convertValue(response.getValue(), clazz);
                DigitalTwinsResponseHeaders twinHeaders = mapper.convertValue(response.getDeserializedHeaders(), DigitalTwinsResponseHeaders.class);
                return new DigitalTwinsResponse<>(response.getRequest(), response.getStatusCode(), response.getHeaders(), genericResponse, twinHeaders);
            });
    }

    /**
     * Gets a relationship on a digital twin.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @param relationshipId The Id of the relationship to retrieve.
     * @return The application/json relationship corresponding to the provided relationshipId.
     */
    @ServiceMethod(returns = ReturnType.SINGLE)
    public Mono<String> getRelationship(String digitalTwinId, String relationshipId) {
        return withContext(context -> getRelationship(digitalTwinId, relationshipId, context))
            .map(DigitalTwinsResponse::getValue);
    }

    /**
     * Gets a relationship on a digital twin.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @param relationshipId The Id of the relationship to retrieve.
     * @return A REST response containing the application/json relationship corresponding to the provided relationshipId.
     */
    @ServiceMethod(returns = ReturnType.SINGLE)
    public Mono<DigitalTwinsResponse<String>> getRelationshipWithResponse(String digitalTwinId, String relationshipId) {
        return withContext(context -> getRelationship(digitalTwinId, relationshipId, context));
    }

    Mono<DigitalTwinsResponse<String>> getRelationship(String digitalTwinId, String relationshipId, Context context) {
        return protocolLayer
            .getDigitalTwins()
            .getRelationshipByIdWithResponseAsync(digitalTwinId, relationshipId, context)
            .flatMap(response -> {
                try {
                    String jsonResponse = mapper.writeValueAsString(response.getValue());
                    DigitalTwinsResponseHeaders twinHeaders = mapper.convertValue(response.getDeserializedHeaders(), DigitalTwinsResponseHeaders.class);
                    return Mono.justOrEmpty(new DigitalTwinsResponse<>(response.getRequest(), response.getStatusCode(), response.getHeaders(), jsonResponse, twinHeaders));
                } catch (JsonProcessingException e) {
                    return Mono.error(e);
                }
            });
    }

    /**
     * Gets a relationship on a digital twin.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @param relationshipId The Id of the relationship to retrieve.
     * @param clazz The model class to convert the relationship to.
     * @param <T> The generic type to convert the relationship to.
     * @return The relationship corresponding to the provided relationshipId.
     */
    @ServiceMethod(returns = ReturnType.SINGLE)
    public <T> Mono<T> getRelationship(String digitalTwinId, String relationshipId, Class<T> clazz) {
        return withContext(context -> getRelationship(digitalTwinId, relationshipId, clazz, context))
            .map(DigitalTwinsResponse::getValue);
    }

    /**
     * Gets a relationship on a digital twin.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @param relationshipId The Id of the relationship to retrieve.
     * @param clazz The model class to convert the relationship to.
     * @param <T> The generic type to convert the relationship to.
     * @return A REST response containing the relationship corresponding to the provided relationshipId.
     */
    @ServiceMethod(returns = ReturnType.SINGLE)
    public <T> Mono<DigitalTwinsResponse<T>> getRelationshipWithResponse(String digitalTwinId, String relationshipId, Class<T> clazz) {
        return withContext(context -> getRelationship(digitalTwinId, relationshipId, clazz, context));
    }

    <T> Mono<DigitalTwinsResponse<T>> getRelationship(String digitalTwinId, String relationshipId, Class<T> clazz, Context context) {
        return protocolLayer
            .getDigitalTwins()
            .getRelationshipByIdWithResponseAsync(digitalTwinId, relationshipId, context)
            .map(response -> {
                T genericResponse = mapper.convertValue(response.getValue(), clazz);
                DigitalTwinsResponseHeaders twinHeaders = mapper.convertValue(response.getDeserializedHeaders(), DigitalTwinsResponseHeaders.class);
                return new DigitalTwinsResponse<>(response.getRequest(), response.getStatusCode(), response.getHeaders(), genericResponse, twinHeaders);
            });
    }

    /**
     * Updates the properties of a relationship on a digital twin.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @param relationshipId The Id of the relationship to be updated.
     * @param relationshipUpdateOperations The application/json-patch+json operations to be performed on the specified digital twin's relationship.
     * @return An empty response.
     */
    @ServiceMethod(returns = ReturnType.SINGLE)
    public Mono<Void> updateRelationship(String digitalTwinId, String relationshipId, List<Object> relationshipUpdateOperations) {
        return withContext(context -> updateRelationship(digitalTwinId, relationshipId, relationshipUpdateOperations, new RequestOptions(), context))
            .flatMap(voidResponse -> Mono.empty());
    }

    /**
     * Updates the properties of a relationship on a digital twin.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @param relationshipId The Id of the relationship to be updated.
     * @param relationshipUpdateOperations The application/json-patch+json operations to be performed on the specified digital twin's relationship.
     * @param options The optional settings for this request.
     * @return A REST response.
     */
    @ServiceMethod(returns = ReturnType.SINGLE)
    public Mono<DigitalTwinsResponse<Void>> updateRelationshipWithResponse(String digitalTwinId, String relationshipId, List<Object> relationshipUpdateOperations, RequestOptions options) {
        return withContext(context -> updateRelationship(digitalTwinId, relationshipId, relationshipUpdateOperations, options, context));
    }

    Mono<DigitalTwinsResponse<Void>> updateRelationship(String digitalTwinId, String relationshipId, List<Object> relationshipUpdateOperations, RequestOptions options, Context context) {
        return protocolLayer
            .getDigitalTwins()
            .updateRelationshipWithResponseAsync(digitalTwinId, relationshipId, options.getIfMatch(), relationshipUpdateOperations, context)
            .map(response -> {
                DigitalTwinsResponseHeaders twinHeaders = mapper.convertValue(response.getDeserializedHeaders(), DigitalTwinsResponseHeaders.class);
                return new DigitalTwinsResponse<>(response.getRequest(), response.getStatusCode(), response.getHeaders(), response.getValue(), twinHeaders);
            });
    }

    /**
     * Deletes a relationship on a digital twin.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @param relationshipId The Id of the relationship to delete.
     * @return An empty response.
     */
    @ServiceMethod(returns = ReturnType.SINGLE)
    public Mono<Void> deleteRelationship(String digitalTwinId, String relationshipId) {
        return withContext(context -> deleteRelationship(digitalTwinId, relationshipId, new RequestOptions(), context))
            .flatMap(voidResponse -> Mono.empty());
    }

    /**
     * Deletes a relationship on a digital twin.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @param relationshipId The Id of the relationship to delete.
     * @param options The optional settings for this request.
     * @return A REST response.
     */
    @ServiceMethod(returns = ReturnType.SINGLE)
    public Mono<Response<Void>> deleteRelationshipWithResponse(String digitalTwinId, String relationshipId, RequestOptions options) {
        return withContext(context -> deleteRelationship(digitalTwinId, relationshipId, options, context));
    }

    Mono<Response<Void>> deleteRelationship(String digitalTwinId, String relationshipId, RequestOptions options, Context context) {
        return protocolLayer
            .getDigitalTwins()
            .deleteRelationshipWithResponseAsync(digitalTwinId, relationshipId, options.getIfMatch(), context);
    }

    /**
     * Gets all the relationships on a digital twin by iterating through a collection.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @return A {@link PagedFlux} of application/json relationships belonging to the specified digital twin and the http response.
     */
    @ServiceMethod(returns = ReturnType.COLLECTION)
    public PagedFlux<String> listRelationships(String digitalTwinId) {
        return listRelationships(digitalTwinId, (String) null);
    }

    /**
     * Gets all the relationships on a digital twin filtered by the relationship name, by iterating through a collection.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @param relationshipName The name of a relationship to filter to.
     * @return A {@link PagedFlux} of application/json relationships belonging to the specified digital twin and the http response.
     */
    @ServiceMethod(returns = ReturnType.COLLECTION)
    public PagedFlux<String> listRelationships(String digitalTwinId, String relationshipName) {
        return new PagedFlux<>(
            () -> withContext(context -> listRelationshipsFirstPage(digitalTwinId, relationshipName, context)),
            nextLink -> withContext(context -> listRelationshipsNextPage(nextLink, context)));
    }

    Mono<PagedResponse<String>> listRelationshipsFirstPage(String digitalTwinId, String relationshipName, Context context) {
        return protocolLayer
            .getDigitalTwins()
            .listRelationshipsSinglePageAsync(digitalTwinId, relationshipName, context)
            .map(
                objectPagedResponse -> {
                    List<String> stringList = objectPagedResponse.getValue().stream()
                        .map(object -> {
                            try {
                                return mapper.writeValueAsString(object);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException("JsonProcessingException occurred while retrieving relationships", e);
                            }
                        })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                    return new PagedResponseBase<>(
                        objectPagedResponse.getRequest(),
                        objectPagedResponse.getStatusCode(),
                        objectPagedResponse.getHeaders(),
                        stringList,
                        objectPagedResponse.getContinuationToken(),
                        ((PagedResponseBase) objectPagedResponse).getDeserializedHeaders());

                }
            );
    }

    Mono<PagedResponse<String>> listRelationshipsNextPage(String nextLink, Context context) {
        return protocolLayer
            .getDigitalTwins()
            .listRelationshipsNextSinglePageAsync(nextLink, context)
            .map(objectPagedResponse -> {
                List<String> stringList = objectPagedResponse.getValue().stream()
                    .map(object -> {
                        try {
                            return mapper.writeValueAsString(object);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException("JsonProcessingException occurred while retrieving relationships", e);
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
                return new PagedResponseBase<>(
                    objectPagedResponse.getRequest(),
                    objectPagedResponse.getStatusCode(),
                    objectPagedResponse.getHeaders(),
                    stringList,
                    objectPagedResponse.getContinuationToken(),
                    ((PagedResponseBase)objectPagedResponse).getDeserializedHeaders());
            });
    }

    PagedFlux<String> listRelationships(String digitalTwinId, String relationshipName, Context context) {
        return new PagedFlux<>(
            () -> listRelationshipsFirstPage(digitalTwinId, relationshipName, context),
            nextLink -> listRelationshipsNextPage(nextLink, context));
    }

    /**
     * Gets all the relationships on a digital twin by iterating through a collection.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @param clazz The model class to convert the relationship to. Since a digital twin might have relationships conforming to different models, it is advisable to convert them to a generic model like {@link com.azure.digitaltwins.core.implementation.serialization.BasicRelationship}.
     * @param <T> The generic type to convert the relationship to.
     * @return A {@link PagedFlux} of relationships belonging to the specified digital twin and the http response.
     */
    @ServiceMethod(returns = ReturnType.COLLECTION)
    public <T> PagedFlux<T> listRelationships(String digitalTwinId, Class<T> clazz) {
        return listRelationships(digitalTwinId, null, clazz);
    }

    /**
     * Gets all the relationships on a digital twin filtered by the relationship name, by iterating through a collection.
     *
     * @param digitalTwinId The Id of the source digital twin.
     * @param relationshipName The name of a relationship to filter to.
     * @param clazz The model class to convert the relationship to.
     * @param <T> The generic type to convert the relationship to.
     * @return A {@link PagedFlux} of relationships belonging to the specified digital twin and the http response.
     */
    @ServiceMethod(returns = ReturnType.COLLECTION)
    public <T> PagedFlux<T> listRelationships(String digitalTwinId, String relationshipName, Class<T> clazz) {
        return new PagedFlux<>(
            () -> withContext(context -> listRelationshipsFirstPage(digitalTwinId, relationshipName, clazz, context)),
            nextLink -> withContext(context -> listRelationshipsNextPage(nextLink, clazz, context)));
    }

    <T> Mono<PagedResponse<T>> listRelationshipsFirstPage(String digitalTwinId, String relationshipName, Class<T> clazz, Context context) {
        return protocolLayer
            .getDigitalTwins()
            .listRelationshipsSinglePageAsync(digitalTwinId, relationshipName, context)
            .map(
                objectPagedResponse -> {
                    List<T> list = objectPagedResponse.getValue().stream()
                        .map(object -> mapper.convertValue(object, clazz))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                    return new PagedResponseBase<>(
                        objectPagedResponse.getRequest(),
                        objectPagedResponse.getStatusCode(),
                        objectPagedResponse.getHeaders(),
                        list,
                        objectPagedResponse.getContinuationToken(),
                        ((PagedResponseBase) objectPagedResponse).getDeserializedHeaders());

                }
            );
    }

    <T> Mono<PagedResponse<T>> listRelationshipsNextPage(String nextLink, Class<T> clazz, Context context) {
        return protocolLayer
            .getDigitalTwins()
            .listRelationshipsNextSinglePageAsync(nextLink, context)
            .map(objectPagedResponse -> {
                List<T> stringList = objectPagedResponse.getValue().stream()
                    .map(object -> mapper.convertValue(object, clazz))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
                return new PagedResponseBase<>(
                    objectPagedResponse.getRequest(),
                    objectPagedResponse.getStatusCode(),
                    objectPagedResponse.getHeaders(),
                    stringList,
                    objectPagedResponse.getContinuationToken(),
                    ((PagedResponseBase)objectPagedResponse).getDeserializedHeaders());
            });
    }

    <T> PagedFlux<T> listRelationships(String digitalTwinId, String relationshipName, Class<T> clazz, Context context) {
        return new PagedFlux<>(
            () -> listRelationshipsFirstPage(digitalTwinId, relationshipName, clazz, context),
            nextLink -> listRelationshipsNextPage(nextLink, clazz, context));
    }

    /**
     * Gets all the relationships referencing a digital twin as a target by iterating through a collection.
     *
     * @param digitalTwinId The Id of the target digital twin.
     * @return A {@link PagedFlux} of relationships directed towards the specified digital twin and the http response.
     */
    @ServiceMethod(returns = ReturnType.COLLECTION)
    public PagedFlux<IncomingRelationship> listIncomingRelationships(String digitalTwinId) {
        return new PagedFlux<>(
            () -> withContext(context -> protocolLayer.getDigitalTwins().listIncomingRelationshipsSinglePageAsync(digitalTwinId, context)),
            nextLink -> withContext(context -> protocolLayer.getDigitalTwins().listIncomingRelationshipsNextSinglePageAsync(nextLink, context)));
    }

    PagedFlux<IncomingRelationship> listIncomingRelationships(String digitalTwinId, Context context) {
        return new PagedFlux<>(
            () -> protocolLayer.getDigitalTwins().listIncomingRelationshipsSinglePageAsync(digitalTwinId, context),
            nextLink -> protocolLayer.getDigitalTwins().listIncomingRelationshipsNextSinglePageAsync(nextLink, context));
    }

}
