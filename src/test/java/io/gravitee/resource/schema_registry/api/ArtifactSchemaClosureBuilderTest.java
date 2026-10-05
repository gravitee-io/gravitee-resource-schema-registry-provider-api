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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArtifactSchemaClosureBuilderTest {

    private ArtifactContentFetcher fetcher;
    private ArtifactSchemaClosureBuilder builder;

    @BeforeEach
    void setUp() {
        fetcher = mock(ArtifactContentFetcher.class);
        builder = new ArtifactSchemaClosureBuilder(fetcher, ArtifactSchemaClosureLimits.defaults());
    }

    @Test
    void should_build_self_contained_bundle_when_references_empty() {
        byte[] root = "<xs:schema/>".getBytes(StandardCharsets.UTF_8);
        when(fetcher.fetchContent("g", "root", "1")).thenReturn(Maybe.just(root));
        when(fetcher.fetchReferences("g", "root", "1")).thenReturn(Single.just(List.of()));

        ArtifactSchemaBundle bundle = builder.build("g", "root", "1").blockingGet();

        assertThat(bundle.rootContent()).isEqualTo(root);
        assertThat(bundle.documentsByName()).isEmpty();
        assertThat(bundle.digest()).isNotBlank();
    }

    @Test
    void should_fetch_referenced_artifacts_recursively() {
        byte[] root = "root".getBytes(StandardCharsets.UTF_8);
        byte[] base = "base".getBytes(StandardCharsets.UTF_8);
        byte[] foundation = "foundation".getBytes(StandardCharsets.UTF_8);

        when(fetcher.fetchContent("FIXM", "nas", "3.0.0")).thenReturn(Maybe.just(root));
        when(fetcher.fetchReferences("FIXM", "nas", "3.0.0"))
            .thenReturn(Single.just(List.of(new ArtifactReference("FIXM", "base", "3.0.1", "Base.xsd"))));
        when(fetcher.fetchContent("FIXM", "base", "3.0.1")).thenReturn(Maybe.just(base));
        when(fetcher.fetchReferences("FIXM", "base", "3.0.1"))
            .thenReturn(Single.just(List.of(new ArtifactReference("FIXM", "foundation", "3.0.1", "Foundation.xsd"))));
        when(fetcher.fetchContent("FIXM", "foundation", "3.0.1")).thenReturn(Maybe.just(foundation));
        when(fetcher.fetchReferences("FIXM", "foundation", "3.0.1")).thenReturn(Single.just(List.of()));

        ArtifactSchemaBundle bundle = builder.build("FIXM", "nas", "3.0.0").blockingGet();

        assertThat(bundle.documentsByName()).containsEntry("Base.xsd", base).containsEntry("Foundation.xsd", foundation);
    }

    @Test
    void should_fail_when_closure_artifact_limit_exceeded() {
        builder = new ArtifactSchemaClosureBuilder(fetcher, new ArtifactSchemaClosureLimits(1, 10_000_000L, 10, 10_000L, 60_000L));

        when(fetcher.fetchContent(anyString(), anyString(), anyString())).thenReturn(Maybe.just("x".getBytes(StandardCharsets.UTF_8)));
        when(fetcher.fetchReferences("g", "root", "1"))
            .thenReturn(Single.just(List.of(new ArtifactReference("g", "child", "1", "Child.xsd"))));
        when(fetcher.fetchReferences("g", "child", "1")).thenReturn(Single.just(List.of()));

        builder
            .build("g", "root", "1")
            .test()
            .assertError(error -> {
                if (!(error instanceof SchemaClosureLimitExceededException)) {
                    return false;
                }
                return (
                    ((SchemaClosureLimitExceededException) error).getLimitKind() ==
                    SchemaClosureLimitExceededException.ClosureLimitKind.ARTIFACTS
                );
            });
    }

    @Test
    void should_produce_deterministic_digest() {
        byte[] root = "<xs:schema/>".getBytes(StandardCharsets.UTF_8);
        when(fetcher.fetchContent("g", "a", "1")).thenReturn(Maybe.just(root));
        when(fetcher.fetchReferences("g", "a", "1")).thenReturn(Single.just(List.of()));

        ArtifactSchemaBundle bundle1 = builder.build("g", "a", "1").blockingGet();
        ArtifactSchemaBundle bundle2 = builder.build("g", "a", "1").blockingGet();

        assertThat(bundle1.digest()).isEqualTo(bundle2.digest());
    }

    @Test
    void should_fail_when_per_artifact_fetch_times_out() throws InterruptedException {
        builder = new ArtifactSchemaClosureBuilder(fetcher, new ArtifactSchemaClosureLimits(50, 10_000_000L, 10, 50L, 60_000L));

        when(fetcher.fetchContent("g", "slow", "1")).thenReturn(Maybe.never());

        builder
            .build("g", "slow", "1")
            .test()
            .awaitDone(2, java.util.concurrent.TimeUnit.SECONDS)
            .assertError(error -> {
                assertThat(error).isInstanceOf(SchemaRegistryUnreachableException.class);
                assertThat(error.getMessage()).contains("Per-artifact fetch timeout");
                return true;
            });
    }

    @Test
    void should_fail_when_total_closure_fetch_times_out() throws InterruptedException {
        // Per-artifact budget is large; overall walk budget is tiny so the outer timeout wins.
        builder = new ArtifactSchemaClosureBuilder(fetcher, new ArtifactSchemaClosureLimits(50, 10_000_000L, 10, 60_000L, 50L));

        when(fetcher.fetchContent("g", "root", "1")).thenReturn(Maybe.never());

        builder
            .build("g", "root", "1")
            .test()
            .awaitDone(2, java.util.concurrent.TimeUnit.SECONDS)
            .assertError(error -> {
                assertThat(error).isInstanceOf(SchemaRegistryUnreachableException.class);
                assertThat(error.getMessage()).contains("Total closure fetch timeout");
                return true;
            });
    }

    @Test
    void should_return_empty_when_root_content_not_found() {
        when(fetcher.fetchContent("g", "missing", "1")).thenReturn(Maybe.empty());
        when(fetcher.fetchReferences("g", "missing", "1")).thenReturn(Single.just(List.of()));

        builder.build("g", "missing", "1").test().assertNoValues().assertComplete();
    }

    @Test
    void should_not_reuse_partial_state_across_subscriptions() {
        byte[] root = "root".getBytes(StandardCharsets.UTF_8);
        byte[] c1 = "c1".getBytes(StandardCharsets.UTF_8);
        byte[] c2 = "c2".getBytes(StandardCharsets.UTF_8);

        when(fetcher.fetchContent("g", "root", "1")).thenReturn(Maybe.just(root));
        when(fetcher.fetchReferences("g", "root", "1"))
            .thenReturn(
                Single.just(List.of(new ArtifactReference("g", "c1", "1", "C1.xsd"), new ArtifactReference("g", "c2", "1", "C2.xsd")))
            );
        when(fetcher.fetchContent("g", "c1", "1")).thenReturn(Maybe.just(c1));
        when(fetcher.fetchReferences("g", "c1", "1")).thenReturn(Single.just(List.of()));
        when(fetcher.fetchContent("g", "c2", "1"))
            .thenReturn(Maybe.error(new SchemaRegistryUnreachableException("boom")))
            .thenReturn(Maybe.just(c2));
        when(fetcher.fetchReferences("g", "c2", "1")).thenReturn(Single.just(List.of()));

        Maybe<ArtifactSchemaBundle> cold = builder.build("g", "root", "1");
        cold.test().assertError(SchemaRegistryUnreachableException.class);

        ArtifactSchemaBundle bundle = cold.blockingGet();
        assertThat(bundle.documentsByName()).containsEntry("C1.xsd", c1).containsEntry("C2.xsd", c2);
    }

    @Test
    void should_keep_aliases_for_same_coordinate_and_fail_on_name_collision() {
        byte[] root = "root".getBytes(StandardCharsets.UTF_8);
        byte[] shared = "shared".getBytes(StandardCharsets.UTF_8);
        byte[] other = "other".getBytes(StandardCharsets.UTF_8);

        when(fetcher.fetchContent("g", "root", "1")).thenReturn(Maybe.just(root));
        when(fetcher.fetchReferences("g", "root", "1"))
            .thenReturn(
                Single.just(
                    List.of(
                        new ArtifactReference("g", "shared", "1", "types.xsd"),
                        new ArtifactReference("g", "shared", "1", "../shared/types-v2.xsd")
                    )
                )
            );
        when(fetcher.fetchContent("g", "shared", "1")).thenReturn(Maybe.just(shared));
        when(fetcher.fetchReferences("g", "shared", "1")).thenReturn(Single.just(List.of()));

        ArtifactSchemaBundle aliases = builder.build("g", "root", "1").blockingGet();
        assertThat(aliases.documentsByName()).containsEntry("types.xsd", shared).containsEntry("../shared/types-v2.xsd", shared);

        when(fetcher.fetchReferences("g", "root", "1"))
            .thenReturn(
                Single.just(List.of(new ArtifactReference("g", "a", "1", "types.xsd"), new ArtifactReference("g", "b", "1", "types.xsd")))
            );
        when(fetcher.fetchContent("g", "a", "1")).thenReturn(Maybe.just(shared));
        when(fetcher.fetchReferences("g", "a", "1")).thenReturn(Single.just(List.of()));
        when(fetcher.fetchContent("g", "b", "1")).thenReturn(Maybe.just(other));
        when(fetcher.fetchReferences("g", "b", "1")).thenReturn(Single.just(List.of()));

        builder
            .build("g", "root", "1")
            .test()
            .assertError(error -> {
                assertThat(error).isInstanceOf(SchemaLoadException.class);
                assertThat(error.getMessage()).contains("maps to multiple coordinates");
                return true;
            });
    }

    @Test
    void should_use_length_prefixed_digest_to_avoid_ambiguity() {
        byte[] ambiguousRoot = "<a/>Base.xsd<b/>".getBytes(StandardCharsets.UTF_8);
        byte[] shortRoot = "<a/>".getBytes(StandardCharsets.UTF_8);
        byte[] doc = "<b/>".getBytes(StandardCharsets.UTF_8);

        String digestNoDocs = ArtifactSchemaClosureBuilder.digest(ambiguousRoot, Map.of());
        String digestWithDoc = ArtifactSchemaClosureBuilder.digest(shortRoot, Map.of("Base.xsd", doc));

        assertThat(digestNoDocs).isNotEqualTo(digestWithDoc);
    }
}
