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
 * Thrown for operational registry unavailability: transport failure, timeout, HTTP 5xx,
 * auth failure after ready (HTTP 401/403), and the negative-cache / backoff window.
 * <p>
 * Distinct from coordinate misses ({@link SchemaArtifactNotFoundException}) and closure limits
 * ({@link SchemaClosureLimitExceededException}), which must not enter the negative-cache window.
 */
public class SchemaRegistryUnreachableException extends SchemaLoadException {

    public SchemaRegistryUnreachableException(String message) {
        super(message);
    }

    public SchemaRegistryUnreachableException(String message, Throwable cause) {
        super(message, cause);
    }
}
