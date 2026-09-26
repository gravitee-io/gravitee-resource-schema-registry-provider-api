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

import io.reactivex.rxjava3.core.Maybe;

/**
 * Capability for group/artifact/version schema lookup (Solace Schema Registry / Apicurio v3).
 * <p>
 * Implemented <em>alongside</em> {@link SchemaRegistryResource} so one resource instance can serve
 * both subject/id consumers and artifact consumers. Policies resolve this capability via
 * {@code resourceManager.getResource(name, ArtifactSchemaLookup.class)}.
 * <p>
 * {@code Maybe} semantics:
 * <ul>
 *   <li>{@code Maybe.empty()} — artifact not found (e.g. registry HTTP 404)</li>
 *   <li>{@code Maybe.error(...)} — transport failure, timeout, auth failure after ready, closure limit, etc.</li>
 *   <li>Not ready — callers must check {@link #isReady()} first; never encoded as empty</li>
 * </ul>
 */
public interface ArtifactSchemaLookup {
    /**
     * Fetch the root artifact and, when the registry returns references, the full import closure.
     *
     * @param groupId schema group
     * @param artifactId schema artifact id
     * @param version pinned version
     * @param ignoreCache when true, bypass positive byte cache (still may update LKG on success)
     * @return empty if not found; error on operational failure; bundle on success
     */
    Maybe<ArtifactSchemaBundle> getArtifactSchema(String groupId, String artifactId, String version, boolean ignoreCache);

    /**
     * {@code true} when {@code start()} completed successfully and the client is usable.
     */
    boolean isReady();
}
