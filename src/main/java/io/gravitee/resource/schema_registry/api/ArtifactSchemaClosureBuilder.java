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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
 */
public final class ArtifactSchemaClosureBuilder {

    private final ArtifactContentFetcher fetcher;
    private final ArtifactSchemaClosureLimits limits;

    public ArtifactSchemaClosureBuilder(ArtifactContentFetcher fetcher, ArtifactSchemaClosureLimits limits) {
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    public Maybe<ArtifactSchemaBundle> build(String groupId, String artifactId, String version) {
        long timeoutMs = limits.totalClosureFetchTimeoutMs();
        Map<String, byte[]> documentsByName = new LinkedHashMap<>();
        Set<String> visited = new LinkedHashSet<>();
        long[] totalBytes = new long[] { 0L };

        return fetchContent(groupId, artifactId, version)
            .flatMap(rootContent -> {
                totalBytes[0] += rootContent.length;
                if (totalBytes[0] > limits.maxClosureBytes()) {
                    return Maybe.error(limitBytes());
                }
                visited.add(coordinateKey(groupId, artifactId, version));
                return fetchReferences(groupId, artifactId, version)
                    .flatMapCompletable(refs -> fetchAll(refs, groupId, 1, visited, documentsByName, totalBytes))
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
        long[] totalBytes
    ) {
        if (refs == null || refs.isEmpty()) {
            return Completable.complete();
        }
        return Flowable
            .fromIterable(refs)
            .concatMapCompletable(ref -> fetchRecursive(ref, defaultGroup, depth, visited, documentsByName, totalBytes));
    }

    private Completable fetchRecursive(
        ArtifactReference ref,
        String defaultGroup,
        int depth,
        Set<String> visited,
        Map<String, byte[]> documentsByName,
        long[] totalBytes
    ) {
        return Completable.defer(() -> {
            if (ref == null || ref.artifactId() == null || ref.version() == null) {
                return Completable.error(new SchemaLoadException("Invalid reference entry in /references"));
            }
            if (depth > limits.maxImportDepth()) {
                return Completable.error(
                    new SchemaClosureLimitExceededException("Max import depth exceeded (" + limits.maxImportDepth() + ")")
                );
            }
            String groupId = ref.groupId() != null && !ref.groupId().isBlank() ? ref.groupId() : defaultGroup;
            String key = coordinateKey(groupId, ref.artifactId(), ref.version());
            if (!visited.add(key)) {
                return Completable.complete();
            }
            if (visited.size() > limits.maxArtifactsInClosure()) {
                return Completable.error(
                    new SchemaClosureLimitExceededException("Max artifacts in closure exceeded (" + limits.maxArtifactsInClosure() + ")")
                );
            }
            String name = ref.name() != null && !ref.name().isBlank() ? ref.name() : ref.artifactId();
            return fetchContent(groupId, ref.artifactId(), ref.version())
                .switchIfEmpty(Maybe.error(new SchemaArtifactNotFoundException("Referenced schema artifact not found: " + key)))
                .flatMapCompletable(content -> {
                    totalBytes[0] += content.length;
                    if (totalBytes[0] > limits.maxClosureBytes()) {
                        return Completable.error(limitBytes());
                    }
                    documentsByName.put(name, content);
                    return fetchReferences(groupId, ref.artifactId(), ref.version())
                        .flatMapCompletable(nested -> fetchAll(nested, groupId, depth + 1, visited, documentsByName, totalBytes));
                });
        });
    }

    private SchemaClosureLimitExceededException limitBytes() {
        return new SchemaClosureLimitExceededException("Max total closure bytes exceeded (" + limits.maxClosureBytes() + ")");
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

    public static String digest(byte[] rootContent, Map<String, byte[]> documentsByName) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(rootContent);
            for (Map.Entry<String, byte[]> e : documentsByName.entrySet()) {
                md.update(e.getKey().getBytes(StandardCharsets.UTF_8));
                md.update(e.getValue());
            }
            return toHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
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
