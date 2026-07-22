package ru.yandex.devtools.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestTag;
import org.junit.platform.launcher.PostDiscoveryFilter;

public class RuntimeTagFilter implements PostDiscoveryFilter {

    private static final Pattern WHITESPACE_AROUND_PLUS = Pattern.compile("\\s*\\+\\s*");
    private static final Pattern WHITESPACE_TOKENS = Pattern.compile("\\s+");
    private static final Pattern PLUS_ALTERNATIVES = Pattern.compile("\\+");

    private final YaTestNameBase testName;
    private final List<JunitTagsClause> clauses;

    public RuntimeTagFilter(YaTestNameBase testName, List<String> rawExpressions) {
        this.testName = testName;
        this.clauses = JunitTagsClause.parseAll(rawExpressions);
    }

    static String describeExpression(String expr) {
        return JunitTagsClause.parseOne(expr).toString();
    }

    @Override
    public FilterResult apply(TestDescriptor test) {
        if (clauses.isEmpty()) {
            return FilterResult.included("");
        }
        if (!testName.isTest(test)) {
            return FilterResult.excluded("No suitable tags found");
        }

        Set<String> tagSet = collectInheritedTags(test);
        for (JunitTagsClause clause : clauses) {
            if (clause.matches(tagSet)) {
                return FilterResult.included("Matched junit-tags clause: " + clause);
            }
        }
        return FilterResult.excluded("No suitable tags found");
    }

    private static Set<String> collectInheritedTags(TestDescriptor test) {
        Set<String> result = new HashSet<>();
        TestDescriptor current = test;
        while (current != null) {
            for (TestTag testTag : current.getTags()) {
                result.add(testTag.getName());
            }
            current = current.getParent().orElse(null);
        }
        return result;
    }

    private static final class JunitTagsClause {
        private final TagSegment positiveSegment;
        private final List<TagSegment> forbiddenSegments;

        private JunitTagsClause(TagSegment positiveSegment, List<TagSegment> forbiddenSegments) {
            this.positiveSegment = positiveSegment;
            this.forbiddenSegments = forbiddenSegments;
        }

        static List<JunitTagsClause> parseAll(List<String> rawExpressions) {
            List<JunitTagsClause> out = new ArrayList<>();
            for (String raw : rawExpressions) {
                if (raw == null) {
                    continue;
                }
                String trimmed = unquote(raw.trim());
                if (trimmed.isEmpty()) {
                    continue;
                }
                out.add(parseOne(trimmed));
            }
            return out;
        }

        private static String unquote(String value) {
            if (value.length() < 2) {
                return value;
            }
            char first = value.charAt(0);
            char last = value.charAt(value.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                return value.substring(1, value.length() - 1).trim();
            }
            return value;
        }

        static JunitTagsClause parseOne(String expr) {
            List<TagSegment> segments = parseSegments(expr);
            TagSegment positive = segments.get(0);
            List<TagSegment> forbidden = new ArrayList<>(segments.subList(1, segments.size()));
            return new JunitTagsClause(positive, forbidden);
        }

        private static List<TagSegment> parseSegments(String expr) {
            String normalized = WHITESPACE_AROUND_PLUS.matcher(unquote(expr.trim())).replaceAll("+");
            if (normalized.isEmpty()) {
                throw invalidExpression(expr, "required part is empty");
            }

            List<String> positiveTerms = new ArrayList<>();
            List<TagSegment> forbiddenSegments = new ArrayList<>();
            for (String token : WHITESPACE_TOKENS.split(normalized)) {
                if (token.startsWith("!")) {
                    String forbiddenTerm = token.substring(1);
                    if (forbiddenTerm.isEmpty()) {
                        throw invalidExpression(expr, "exclusion tag is empty");
                    }
                    forbiddenSegments.add(TagSegment.parse(List.of(forbiddenTerm), expr, "exclusion part"));
                    continue;
                }
                positiveTerms.add(token);
            }
            List<TagSegment> segments = new ArrayList<>();
            if (positiveTerms.isEmpty()) {
                if (forbiddenSegments.isEmpty()) {
                    throw invalidExpression(expr, "required part is empty");
                }
                segments.add(TagSegment.matchAll());
            } else {
                segments.add(TagSegment.parse(positiveTerms, expr, "required part"));
            }
            segments.addAll(forbiddenSegments);
            return segments;
        }

        boolean matches(Set<String> tagsOnTest) {
            if (!positiveSegment.matches(tagsOnTest)) {
                return false;
            }
            for (TagSegment forbiddenSegment : forbiddenSegments) {
                if (forbiddenSegment.matches(tagsOnTest)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            if (!positiveSegment.isMatchAll()) {
                builder.append(positiveSegment);
            }
            for (TagSegment forbiddenSegment : forbiddenSegments) {
                if (builder.length() > 0) {
                    builder.append(' ');
                }
                builder.append('!').append(forbiddenSegment);
            }
            return builder.toString();
        }
    }

    private static final class TagSegment {
        private final List<TagTerm> terms;
        private final boolean matchAll;

        private TagSegment(List<TagTerm> terms, boolean matchAll) {
            this.terms = terms;
            this.matchAll = matchAll;
        }

        static TagSegment matchAll() {
            return new TagSegment(List.of(), true);
        }

        boolean isMatchAll() {
            return matchAll;
        }

        static TagSegment parse(List<String> rawTerms, String expression, String partName) {
            if (rawTerms.isEmpty()) {
                throw invalidExpression(expression, partName + " is empty");
            }

            List<TagTerm> terms = new ArrayList<>();
            for (String rawTerm : rawTerms) {
                terms.add(TagTerm.parse(rawTerm, expression));
            }
            return new TagSegment(terms, false);
        }

        boolean matches(Set<String> tagsOnTest) {
            if (matchAll) {
                return true;
            }
            for (TagTerm term : terms) {
                if (!term.matches(tagsOnTest)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            return terms.stream()
                    .map(TagTerm::toString)
                    .collect(Collectors.joining(" "));
        }
    }

    private static final class TagTerm {
        private final List<String> alternativeTags;

        private TagTerm(List<String> alternativeTags) {
            this.alternativeTags = alternativeTags;
        }

        static TagTerm parse(String rawTerm, String expression) {
            List<String> alternativeTags = Arrays.stream(PLUS_ALTERNATIVES.split(rawTerm))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
            if (alternativeTags.isEmpty()) {
                throw invalidExpression(expression, "term is empty");
            }
            return new TagTerm(alternativeTags);
        }

        boolean matches(Set<String> tagsOnTest) {
            for (String tag : alternativeTags) {
                if (tagsOnTest.contains(tag)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            return String.join("+", alternativeTags);
        }
    }

    private static IllegalArgumentException invalidExpression(String expression, String reason) {
        return new IllegalArgumentException("Invalid --junit-tags expression '" + expression + "': " + reason);
    }
}
