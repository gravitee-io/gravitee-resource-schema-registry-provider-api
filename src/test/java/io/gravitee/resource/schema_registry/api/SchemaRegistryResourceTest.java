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

import static org.assertj.core.api.Assertions.assertThat;

import io.gravitee.resource.api.ResourceConfiguration;
import org.junit.jupiter.api.Test;

class SchemaRegistryResourceTest {

    @Test
    void defaultGetSchemaByArtifactShouldResolveEmpty() {
        SchemaRegistryResource<?> resource = new SchemaRegistryResourceStub();

        assertThat(resource.getSchemaByArtifact("group", "artifact", "1").blockingGet()).isNull();
    }

    @Test
    void shouldExposeXsdSchemaType() {
        assertThat(SchemaType.valueOf("XSD")).isEqualTo(SchemaType.XSD);
    }

    private static final class SchemaRegistryResourceStub extends SchemaRegistryResource<ResourceConfiguration> {

        @Override
        public io.reactivex.rxjava3.core.Maybe<Schema> getSchemaById(String id) {
            return io.reactivex.rxjava3.core.Maybe.empty();
        }

        @Override
        public io.reactivex.rxjava3.core.Maybe<Schema> getSchema(String subject) {
            return io.reactivex.rxjava3.core.Maybe.empty();
        }

        @Override
        public io.reactivex.rxjava3.core.Maybe<Schema> getSchema(String subject, boolean ignoreCache) {
            return io.reactivex.rxjava3.core.Maybe.empty();
        }

        @Override
        public io.reactivex.rxjava3.core.Maybe<Schema> getSchema(String subject, String version) {
            return io.reactivex.rxjava3.core.Maybe.empty();
        }

        @Override
        public io.reactivex.rxjava3.core.Maybe<Schema> getSchema(String subject, String version, boolean ignoreCache) {
            return io.reactivex.rxjava3.core.Maybe.empty();
        }
    }
}
