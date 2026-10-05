/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.resource.schema_registry.api;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Shared all-or-nothing closure builder for registry-sourced schemas.
 * <p>
 * Used by the Gateway resource and any Management “test resolution” helper — one implementation.
 * <p>
 * Document names are the referrer-supplied {@code name} (or {@code artifactId} when blank). The same
 * coordinate may appear under multiple names (aliases); one name mapping to two coordinates fails
 * the build. Consumers that resolve by basename only see the names present in the bundle.
 */
public final class ArtifactSchemaClosureBuilder {

    private final ArtifactContentFetcher fetcher;
    private final ArtifactSchemaClosureLimits limits;

    public ArtifactSchemaClosureBuilder(ArtifactContentFetcher fetcher, ArtifactSchemaClosureLimits limits) {
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    public Maybe<ArtifactSchemaBundle> build(String groupId, String artifactId, String version) {
        // Allocate closure state per subscription so retry / re-subscribe cannot reuse a partial run.
        return Maybe.defer(() -> {
            long timeoutMs = limits.totalClosureFetchTimeoutMs();
            Map<String, byte[]> documentsByName = new LinkedHashMap<>();
            Map<String, byte[]> contentByCoordinate = new LinkedHashMap<>();
            Map<String, String> nameToCoordinate = new LinkedHashMap<>();
            Set<String> visited = new LinkedHashSet<>();
            long[] totalBytes = new long[] { 0L };

            return fetchContent(groupId, artifactId, version)
                .flatMap(rootContent -> {
                    totalBytes[0] += rootContent.length;
                    if (totalBytes[0] > limits.maxClosureBytes()) {
                        return Maybe.error(limitBytes());
                    }
                    String rootKey = coordinateKey(groupId, artifactId, version);
                    visited.add(rootKey);
                    contentByCoordinate.put(rootKey, rootContent);
                    return fetchReferences(groupId, artifactId, version)
                        .flatMapCompletable(refs ->
                            fetchAll(refs, groupId, 1, visited, documentsByName, contentByCoordinate, nameToCoordinate, totalBytes)
                        )
                        .andThen(Maybe.fromCallable(() -> toBundle(rootContent, documentsByName, groupId, artifactId, version)));
                })
                .timeout(timeoutMs, TimeUnit.MILLISECONDS)
                .onErrorResumeNext(error -> {
                    if (error instanceof java.util.concurrent.TimeoutException) {
                        return Maybe.error(
                            new SchemaRegistryUnreachableException("Total closure fetch timeout exceeded (" + timeoutMs + "ms)", error)
                        );
                    }
                    return Maybe.error(error);
                });
        });
    }

    private Maybe<byte[]> fetchContent(String groupId, String artifactId, String version) {
        long perArtifactMs = limits.perArtifactFetchTimeoutMs();
        return fetcher
            .fetchContent(groupId, artifactId, version)
            .timeout(perArtifactMs, TimeUnit.MILLISECONDS)
            .onErrorResumeNext(error -> {
                if (error instanceof java.util.concurrent.TimeoutException) {
                    return Maybe.error(
                        new SchemaRegistryUnreachableException(
                            "Per-artifact fetch timeout exceeded (" +
                            perArtifactMs +
                            "ms) for " +
                            coordinateKey(groupId, artifactId, version) +
                            " /content",
                            error
                        )
                    );
                }
                return Maybe.error(error);
            });
    }

    private Single<List<ArtifactReference>> fetchReferences(String groupId, String artifactId, String version) {
        long perArtifactMs = limits.perArtifactFetchTimeoutMs();
        return fetcher
            .fetchReferences(groupId, artifactId, version)
            .timeout(perArtifactMs, TimeUnit.MILLISECONDS)
            .onErrorResumeNext(error -> {
                if (error instanceof java.util.concurrent.TimeoutException) {
                    return Single.error(
                        new SchemaRegistryUnreachableException(
                            "Per-artifact fetch timeout exceeded (" +
                            perArtifactMs +
                            "ms) for " +
                            coordinateKey(groupId, artifactId, version) +
                            " /references",
                            error
                        )
                    );
                }
                return Single.error(error);
            });
    }

    private Completable fetchAll(
        List<ArtifactReference> refs,
        String defaultGroup,
        int depth,
        Set<String> visited,
        Map<String, byte[]> documentsByName,
        Map<String, byte[]> contentByCoordinate,
        Map<String, String> nameToCoordinate,
        long[] totalBytes
    ) {
        if (refs == null || refs.isEmpty()) {
            return Completable.complete();
        }
        return Flowable
            .fromIterable(refs)
            .concatMapCompletable(ref ->
                fetchRecursive(ref, defaultGroup, depth, visited, documentsByName, contentByCoordinate, nameToCoordinate, totalBytes)
            );
    }

    private Completable fetchRecursive(
        ArtifactReference ref,
        String defaultGroup,
        int depth,
        Set<String> visited,
        Map<String, byte[]> documentsByName,
        Map<String, byte[]> contentByCoordinate,
        Map<String, String> nameToCoordinate,
        long[] totalBytes
    ) {
        return Completable.defer(() -> {
            if (ref == null || ref.artifactId() == null || ref.version() == null) {
                return Completable.error(new SchemaLoadException("Invalid reference entry in /references"));
            }
            if (depth > limits.maxImportDepth()) {
                return Completable.error(
                    new SchemaClosureLimitExceededException(
                        "Max import depth exceeded (" + limits.maxImportDepth() + ")",
                        SchemaClosureLimitExceededException.ClosureLimitKind.IMPORT_DEPTH
                    )
                );
            }
            String groupId = ref.groupId() != null && !ref.groupId().isBlank() ? ref.groupId() : defaultGroup;
            String key = coordinateKey(groupId, ref.artifactId(), ref.version());
            String name = ref.name() != null && !ref.name().isBlank() ? ref.name() : ref.artifactId();

            if (!visited.add(key)) {
                // Same coordinate under another name: keep an alias so schemaLocation basename lookup works.
                return registerDocumentName(name, key, contentByCoordinate.get(key), documentsByName, nameToCoordinate);
            }
            if (visited.size() > limits.maxArtifactsInClosure()) {
                return Completable.error(
                    new SchemaClosureLimitExceededException(
                        "Max artifacts in closure exceeded (" + limits.maxArtifactsInClosure() + ")",
                        SchemaClosureLimitExceededException.ClosureLimitKind.ARTIFACTS
                    )
                );
            }
            return fetchContent(groupId, ref.artifactId(), ref.version())
                .switchIfEmpty(Maybe.error(new SchemaArtifactNotFoundException("Referenced schema artifact not found: " + key)))
                .flatMapCompletable(content -> {
                    totalBytes[0] += content.length;
                    if (totalBytes[0] > limits.maxClosureBytes()) {
                        return Completable.error(limitBytes());
                    }
                    contentByCoordinate.put(key, content);
                    return registerDocumentName(name, key, content, documentsByName, nameToCoordinate)
                        .andThen(
                            fetchReferences(groupId, ref.artifactId(), ref.version())
                                .flatMapCompletable(nested ->
                                    fetchAll(
                                        nested,
                                        groupId,
                                        depth + 1,
                                        visited,
                                        documentsByName,
                                        contentByCoordinate,
                                        nameToCoordinate,
                                        totalBytes
                                    )
                                )
                        );
                });
        });
    }

    private static Completable registerDocumentName(
        String name,
        String coordinateKey,
        byte[] content,
        Map<String, byte[]> documentsByName,
        Map<String, String> nameToCoordinate
    ) {
        if (content == null) {
            return Completable.error(new SchemaLoadException("Missing content for coordinate: " + coordinateKey));
        }
        String existingCoordinate = nameToCoordinate.get(name);
        if (existingCoordinate != null && !existingCoordinate.equals(coordinateKey)) {
            return Completable.error(
                new SchemaLoadException(
                    "Schema document name '" + name + "' maps to multiple coordinates: " + existingCoordinate + " and " + coordinateKey
                )
            );
        }
        nameToCoordinate.put(name, coordinateKey);
        documentsByName.put(name, content);
        return Completable.complete();
    }

    private SchemaClosureLimitExceededException limitBytes() {
        return new SchemaClosureLimitExceededException(
            "Max total closure bytes exceeded (" + limits.maxClosureBytes() + ")",
            SchemaClosureLimitExceededException.ClosureLimitKind.BYTES
        );
    }

    private static ArtifactSchemaBundle toBundle(
        byte[] rootContent,
        Map<String, byte[]> documentsByName,
        String groupId,
        String artifactId,
        String version
    ) {
        return new ArtifactSchemaBundle(rootContent, documentsByName, digest(rootContent, documentsByName), groupId, artifactId, version);
    }

    /**
     * Length-prefixed SHA-256 over root bytes and each document name/content pair (names sorted).
     * Length prefixes prevent ambiguous concatenations of root vs document segments.
     */
    public static String digest(byte[] rootContent, Map<String, byte[]> documentsByName) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            updateLengthPrefixed(md, rootContent);
            if (documentsByName != null && !documentsByName.isEmpty()) {
                List<String> names = new ArrayList<>(documentsByName.keySet());
                Collections.sort(names);
                for (String name : names) {
                    byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
                    updateLengthPrefixed(md, nameBytes);
                    byte[] content = documentsByName.get(name);
                    Objects.requireNonNull(content, "documentsByName value for " + name);
                    updateLengthPrefixed(md, content);
                }
            }
            return toHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static void updateLengthPrefixed(MessageDigest md, byte[] bytes) {
        md.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
        md.update(bytes);
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    public static String coordinateKey(String groupId, String artifactId, String version) {
        return groupId + "|" + artifactId + "|" + version;
    }
}
