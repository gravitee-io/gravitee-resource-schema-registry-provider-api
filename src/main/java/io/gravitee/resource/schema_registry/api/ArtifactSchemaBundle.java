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

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Bytes-only schema package returned by {@link ArtifactSchemaLookup}.
 * <p>
 * Covers a self-contained root ({@code documentsByName} empty) and a multi-artifact closure
 * (related documents indexed by registry {@code name} / XSD {@code schemaLocation} basename).
 * <p>
 * Defensive copies protect CacheManager / LKG entries from caller mutation. Implements
 * {@link Serializable} so node CacheManager distributed backends can store the value.
 */
public final class ArtifactSchemaBundle implements Serializable {

    private static final long serialVersionUID = 1L;

    private final byte[] rootContent;
    private final Map<String, byte[]> documentsByName;
    private final String digest;
    private final String groupId;
    private final String artifactId;
    private final String version;

    public ArtifactSchemaBundle(
        byte[] rootContent,
        Map<String, byte[]> documentsByName,
        String digest,
        String groupId,
        String artifactId,
        String version
    ) {
        Objects.requireNonNull(rootContent, "rootContent");
        this.rootContent = rootContent.clone();
        Map<String, byte[]> copied = new LinkedHashMap<>();
        if (documentsByName != null) {
            for (Map.Entry<String, byte[]> e : documentsByName.entrySet()) {
                copied.put(e.getKey(), e.getValue() == null ? null : e.getValue().clone());
            }
        }
        this.documentsByName = Collections.unmodifiableMap(copied);
        this.digest = Objects.requireNonNull(digest, "digest");
        this.groupId = groupId;
        this.artifactId = artifactId;
        this.version = version;
    }

    /** Defensive copy of root bytes. */
    public byte[] rootContent() {
        return rootContent.clone();
    }

    /** Unmodifiable map; each value is a defensive copy. */
    public Map<String, byte[]> documentsByName() {
        Map<String, byte[]> copied = new LinkedHashMap<>();
        for (Map.Entry<String, byte[]> e : documentsByName.entrySet()) {
            copied.put(e.getKey(), e.getValue() == null ? null : e.getValue().clone());
        }
        return Collections.unmodifiableMap(copied);
    }

    public String digest() {
        return digest;
    }

    public String groupId() {
        return groupId;
    }

    public String artifactId() {
        return artifactId;
    }

    public String version() {
        return version;
    }
}
