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

/**
 * Thrown when a schema artifact is missing (root or referenced sibling).
 * <p>
 * Root misses from {@link ArtifactSchemaLookup#getArtifactSchema} are expressed as {@code Maybe.empty()}.
 * Sibling misses during an all-or-nothing closure walk are raised as this exception so callers can map
 * to {@code XML_VALIDATION_SCHEMA_ARTIFACT_NOT_FOUND} without string matching.
 */
public class SchemaArtifactNotFoundException extends SchemaLoadException {

    public SchemaArtifactNotFoundException(String message) {
        super(message);
    }
}
