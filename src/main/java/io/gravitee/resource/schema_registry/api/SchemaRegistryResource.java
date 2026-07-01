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

import io.gravitee.resource.api.AbstractConfigurableResource;
import io.gravitee.resource.api.ResourceConfiguration;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import java.util.List;

public abstract class SchemaRegistryResource<C extends ResourceConfiguration> extends AbstractConfigurableResource<C> {

    /**
     * Fetch a schema from the schema registry using its id.
     * @param id The id used to fetch the schema.
     * @return Maybe of Schema
     */
    public abstract Maybe<Schema> getSchemaById(String id);

    /**
     * Fetch the latest version of a schema from the schema registry.
     * @param subject The subject used to fetch the schema.
     * @return Maybe of Schema
     */
    public abstract Maybe<Schema> getSchema(String subject);

    /**
     * Fetch the latest version of a schema from the schema registry.
     * @param subject The subject used to fetch the schema.
     * @param ignoreCache A flag to ignore the potential cache.
     * @return Maybe of Schema
     */
    public abstract Maybe<Schema> getSchema(String subject, boolean ignoreCache);

    /**
     * Fetch a version of a schema from the schema registry.
     * @param subject The subject used to fetch the schema.
     * @param version The wanted version.
     * @return Maybe of Schema
     */
    public abstract Maybe<Schema> getSchema(String subject, String version);

    /**
     * Fetch a version of a schema from the schema registry.
     * @param subject The subject used to fetch the schema
     * @param version The wanted version.
     * @param ignoreCache A flag to ignore the potential cache.
     * @return Maybe of Schema
     */
    public abstract Maybe<Schema> getSchema(String subject, String version, boolean ignoreCache);

    /**
     * Check whether the given schema content is registered under the subject, independently of which
     * version is {@code latest}. When it is, resolves to the registered {@link Schema} carrying its id,
     * version and type; otherwise resolves empty.
     * <p>
     * The default implementation resolves empty so registries that do not support membership lookups
     * degrade gracefully: callers treat "no membership info" as "not a member" and may fall back to
     * their current {@code latest} comparison.
     *
     * @param subject the subject the schema is expected to be registered under.
     * @param schemaContent the raw schema definition to look up.
     * @return Maybe of the registered Schema, or empty if not registered / unsupported.
     */
    public Maybe<Schema> lookupUnderSubject(String subject, String schemaContent) {
        return Maybe.empty();
    }

    /**
     * List the version identifiers registered under the subject. Convenience for diagnostics/UX; the
     * membership primitive ({@link #lookupUnderSubject}) is what the gate needs.
     * <p>
     * The default implementation resolves an empty list for registries that do not support it.
     *
     * @param subject the subject to list versions for.
     * @return Single of the registered version identifiers (possibly empty).
     */
    public Single<List<String>> getVersions(String subject) {
        return Single.just(List.of());
    }
}
