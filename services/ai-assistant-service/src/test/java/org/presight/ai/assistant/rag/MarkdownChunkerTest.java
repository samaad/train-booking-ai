package org.presight.ai.assistant.rag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/** The roadmap's Phase 5 ingestion test: every chunk the real corpus produces stays under 500 tokens. */
class MarkdownChunkerTest {

    private final MarkdownChunker chunker = new MarkdownChunker();

    @Test
    void everyCorpusChunkFitsTheEmbeddingWindow() throws Exception {
        Resource[] corpus = new PathMatchingResourcePatternResolver().getResources("classpath*:rag-corpus/*.md");
        assertThat(corpus).hasSizeGreaterThanOrEqualTo(6);

        List<MarkdownChunker.Chunk> all = new ArrayList<>();
        for (Resource r : corpus) {
            List<MarkdownChunker.Chunk> chunks = chunker.chunk(r.getFilename(), r.getContentAsString(StandardCharsets.UTF_8));
            assertThat(chunks).as(r.getFilename()).isNotEmpty();
            assertThat(chunks).allSatisfy(c -> {
                assertThat(c.metadata()).containsKeys("docType", "validFrom", "validTo", "source", "section");
                assertThat(c.estimatedTokens()).isLessThanOrEqualTo(MarkdownChunker.HARD_LIMIT_TOKENS);
            });
            all.addAll(chunks);
        }
        assertThat(all).allSatisfy(c -> assertThat(c.estimatedTokens()).isLessThanOrEqualTo(MarkdownChunker.TARGET_TOKENS));
    }

    @Test
    void longSectionsAreSplitWithOverlapAndUnderTheTarget() {
        StringBuilder md = new StringBuilder("---\ndocType: policy\n---\n# Long section\n");
        for (int i = 0; i < 120; i++) {
            md.append("Sentence number ").append(i).append(" explains one more detail of the long policy. ");
        }
        List<MarkdownChunker.Chunk> chunks = chunker.chunk("long.md", md.toString());

        assertThat(chunks).hasSizeGreaterThan(2)
                .allSatisfy(c -> assertThat(c.estimatedTokens()).isLessThanOrEqualTo(MarkdownChunker.TARGET_TOKENS))
                .allSatisfy(c -> assertThat(c.text()).startsWith("# Long section"));
        String endOfFirst = chunks.get(0).text().substring(chunks.get(0).text().lastIndexOf("Sentence number"));
        assertThat(chunks.get(1).text()).contains(endOfFirst.trim()); // overlap carries context across the cut
    }

    @Test
    void aChunkTheModelWouldTruncateFailsIngestion() {
        String hugeHeading = "# " + "heading ".repeat(600);
        assertThatThrownBy(() -> chunker.chunk("bad.md", hugeHeading + "\nBody text."))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("silently truncate");
    }
}
