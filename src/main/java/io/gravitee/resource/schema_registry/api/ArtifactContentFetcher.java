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
import io.reactivex.rxjava3.core.Single;
import java.util.List;

/**
 * HTTP transport used by {@link ArtifactSchemaClosureBuilder} (Gateway resource or Management advisory helper).
 */
public interface ArtifactContentFetcher {
    /**
     * GET artifact {@code /content}. Empty on HTTP 404.
     */
    Maybe<byte[]> fetchContent(String groupId, String artifactId, String version);

    /**
     * GET artifact {@code /references}. Empty list on 404 or [].
     */
    Single<List<ArtifactReference>> fetchReferences(String groupId, String artifactId, String version);
}
