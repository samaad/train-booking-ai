package org.presight.ai.assistant.rag;

import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.Encoding;
import com.knuddels.jtokkit.api.EncodingType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Splits markdown into retrieval chunks that fit mxbai-embed-large's <b>512-token</b> window.
 *
 * <p>That model truncates longer input <em>silently</em>: an oversized chunk embeds only its first 512
 * tokens and retrieval quietly degrades without ever erroring. So chunks target ≤ {@value #TARGET_TOKENS}
 * tokens with ~{@value #OVERLAP_TOKENS} tokens of overlap, and any chunk over {@value #HARD_LIMIT_TOKENS}
 * fails ingestion outright.
 *
 * <p>Tokens are estimated with cl100k and inflated by {@value #WORDPIECE_FACTOR}×, because BERT's smaller
 * WordPiece vocabulary produces more tokens than cl100k for the same text. Front matter
 * ({@code key: value} between {@code ---} lines) becomes chunk metadata; each chunk is prefixed with its
 * section heading so it reads on its own.
 */
public final class MarkdownChunker {

    static final int TARGET_TOKENS = 380;
    static final int OVERLAP_TOKENS = 50;
    static final int HARD_LIMIT_TOKENS = 500;
    static final double WORDPIECE_FACTOR = 1.2;

    private static final Encoding CL100K = Encodings.newLazyEncodingRegistry().getEncoding(EncodingType.CL100K_BASE);

    public record Chunk(String text, int estimatedTokens, int index, Map<String, String> metadata) {
    }

    public static int estimateTokens(String text) {
        return (int) Math.ceil(CL100K.countTokens(text) * WORDPIECE_FACTOR);
    }

    public List<Chunk> chunk(String source, String markdown) {
        Map<String, String> frontMatter = new LinkedHashMap<>();
        String body = stripFrontMatter(markdown, frontMatter);

        List<Chunk> chunks = new ArrayList<>();
        for (Section section : sections(body)) {
            String prefix = section.heading().isEmpty() ? "" : section.heading() + "\n";
            for (String text : pack(section.sentences(), estimateTokens(prefix))) {
                String chunkText = prefix + text;
                int tokens = estimateTokens(chunkText);
                if (tokens > HARD_LIMIT_TOKENS) {
                    throw new IllegalStateException("Chunk " + chunks.size() + " of " + source + " is ~" + tokens
                            + " tokens (limit " + HARD_LIMIT_TOKENS + "); mxbai-embed-large would silently truncate it");
                }
                Map<String, String> metadata = new LinkedHashMap<>(frontMatter);
                metadata.put("source", source);
                metadata.put("section", section.heading().replaceFirst("^#+\\s*", ""));
                chunks.add(new Chunk(chunkText, tokens, chunks.size(), metadata));
            }
        }
        return chunks;
    }

    /** Greedily packs sentences up to the target, carrying ~OVERLAP_TOKENS of trailing sentences forward. */
    private static List<String> pack(List<String> sentences, int prefixTokens) {
        int budget = TARGET_TOKENS - prefixTokens;
        List<String> out = new ArrayList<>();
        List<String> current = new ArrayList<>();
        int currentTokens = 0;
        boolean hasNew = false;
        for (String sentence : sentences) {
            for (String piece : splitOversized(sentence, budget)) {
                int t = estimateTokens(piece);
                if (currentTokens + t > budget && hasNew) {
                    out.add(String.join(" ", current));
                    List<String> overlap = tail(current, OVERLAP_TOKENS);
                    current = new ArrayList<>(overlap);
                    currentTokens = overlap.stream().mapToInt(MarkdownChunker::estimateTokens).sum();
                    hasNew = false;
                }
                current.add(piece);
                currentTokens += t;
                hasNew = true;
            }
        }
        if (hasNew) {
            out.add(String.join(" ", current));
        }
        return out;
    }

    private static List<String> tail(List<String> sentences, int maxTokens) {
        List<String> tail = new ArrayList<>();
        int tokens = 0;
        for (int i = sentences.size() - 1; i >= 0; i--) {
            int t = estimateTokens(sentences.get(i));
            if (tokens + t > maxTokens) {
                break;
            }
            tail.addFirst(sentences.get(i));
            tokens += t;
        }
        return tail;
    }

    /** A single run-on "sentence" longer than the budget is cut on word boundaries. */
    private static List<String> splitOversized(String sentence, int budget) {
        if (estimateTokens(sentence) <= budget) {
            return List.of(sentence);
        }
        List<String> pieces = new ArrayList<>();
        StringBuilder piece = new StringBuilder();
        for (String word : sentence.split("\\s+")) {
            if (!piece.isEmpty() && estimateTokens(piece + " " + word) > budget) {
                pieces.add(piece.toString());
                piece.setLength(0);
            }
            piece.append(piece.isEmpty() ? "" : " ").append(word);
        }
        if (!piece.isEmpty()) {
            pieces.add(piece.toString());
        }
        return pieces;
    }

    private record Section(String heading, List<String> sentences) {
    }

    private static List<Section> sections(String body) {
        List<Section> sections = new ArrayList<>();
        String heading = "";
        StringBuilder text = new StringBuilder();
        for (String line : body.split("\n")) {
            if (line.startsWith("#")) {
                addSection(sections, heading, text);
                heading = line.trim();
                text.setLength(0);
            } else {
                text.append(line).append('\n');
            }
        }
        addSection(sections, heading, text);
        return sections;
    }

    private static void addSection(List<Section> sections, String heading, StringBuilder text) {
        List<String> sentences = new ArrayList<>();
        for (String paragraph : text.toString().split("\\n\\s*\\n")) {
            String flat = paragraph.replaceAll("\\s+", " ").trim();
            if (!flat.isEmpty()) {
                for (String s : flat.split("(?<=[.!?])\\s+")) {
                    sentences.add(s);
                }
            }
        }
        if (!sentences.isEmpty()) {
            sections.add(new Section(heading, sentences));
        }
    }

    private static String stripFrontMatter(String markdown, Map<String, String> into) {
        String text = markdown.replace("\r\n", "\n");
        if (!text.startsWith("---\n")) {
            return text;
        }
        int end = text.indexOf("\n---", 4);
        if (end < 0) {
            return text;
        }
        for (String line : text.substring(4, end).split("\n")) {
            int colon = line.indexOf(':');
            if (colon > 0) {
                into.put(line.substring(0, colon).trim(), line.substring(colon + 1).trim());
            }
        }
        return text.substring(end + 4);
    }
}
