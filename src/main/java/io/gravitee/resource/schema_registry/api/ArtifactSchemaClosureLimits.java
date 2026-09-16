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
 * Closure limits for registry-sourced schema packaging.
 */
public final class ArtifactSchemaClosureLimits {

    private final int maxArtifactsInClosure;
    private final long maxClosureBytes;
    private final int maxImportDepth;
    private final long totalClosureFetchTimeoutMs;

    public ArtifactSchemaClosureLimits(
        int maxArtifactsInClosure,
        long maxClosureBytes,
        int maxImportDepth,
        long totalClosureFetchTimeoutMs
    ) {
        this.maxArtifactsInClosure = maxArtifactsInClosure;
        this.maxClosureBytes = maxClosureBytes;
        this.maxImportDepth = maxImportDepth;
        this.totalClosureFetchTimeoutMs = totalClosureFetchTimeoutMs;
    }

    public static ArtifactSchemaClosureLimits defaults() {
        return new ArtifactSchemaClosureLimits(50, 10L * 1024 * 1024, 10, 60_000L);
    }

    public int maxArtifactsInClosure() {
        return maxArtifactsInClosure;
    }

    public long maxClosureBytes() {
        return maxClosureBytes;
    }

    public int maxImportDepth() {
        return maxImportDepth;
    }

    public long totalClosureFetchTimeoutMs() {
        return totalClosureFetchTimeoutMs;
    }
}
