package tech.ytsaurus.core.cypress;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import tech.ytsaurus.yson.YsonParser;
import tech.ytsaurus.yson.YsonToken;
import tech.ytsaurus.yson.YsonTokenType;
import tech.ytsaurus.yson.YsonTokenizer;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Allows creating {@link RichYPath} from its binary representation.
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/storage/ypath">YPath documentation</a>
 */
public class RichYPathParser {
    private RichYPathParser() {
    }

    public static YPath parse(String data) {
        return parse(data.getBytes(StandardCharsets.UTF_8));
    }

    public static YPath parse(byte[] data) {
        YTreeBuilder attributesBuilder = YTree.builder();

        YsonParser parser = new YsonParser(data);
        int offset = parser.parseAttributes(attributesBuilder);

        Map<String, YTreeNode> attributes = new HashMap<>(attributesBuilder.entity().build().getAttributes());

        byte[] pathWithoutAttributes = Arrays.copyOfRange(data, offset, data.length);
        YPathTokenizer tokenizer = new YPathTokenizer(pathWithoutAttributes);
        while (tokenizer.getType() != TokenType.EndOfStream && tokenizer.getType() != TokenType.Range) {
            tokenizer.advance();
        }

        var simplePath = RichYPath.simple(new String(tokenizer.getPrefix(), StandardCharsets.UTF_8));
        var rangeBuffer = tokenizer.getToken();

        if (rangeBuffer != null && tokenizer.getType() == TokenType.Range) {
            YsonTokenizer ysonTokenizer = new YsonTokenizer(rangeBuffer);
            ysonTokenizer.parseNext();

            parseColumns(ysonTokenizer, attributes);
            parseRowRanges(ysonTokenizer, attributes);

            ysonTokenizer.getCurrentToken().expectType(YsonTokenType.EndOfStream);
        }

        if (attributes.isEmpty()) {
            return simplePath;
        }
        return RichYPath.fromAttributes(simplePath, attributes);
    }

    private static Optional<RangeLimit> parseRowLimit(YsonTokenizer tokenizer, List<YsonTokenType> separators) {
        if (separators.contains(tokenizer.getCurrentType())) {
            return Optional.empty();
        }

        RangeLimit rangeLimit;
        switch (tokenizer.getCurrentType()) {
            case Hash: {  // RichYPathTags.ROW_INDEX_MARKER_TOKEN
                tokenizer.parseNext();
                rangeLimit = RangeLimit.row(tokenizer.getCurrentToken().asInt64Value());
                tokenizer.parseNext();
                break;
            }
            case LeftParenthesis: {  // RichYPathTags.BEGIN_TUPLE_TOKEN
                tokenizer.parseNext();
                List<YTreeNode> keys = new ArrayList<>();
                while (tokenizer.getCurrentType() != RichYPathTags.END_TUPLE_TOKEN) {
                    keys.add(parseKeyPart(tokenizer));
                    switch (tokenizer.getCurrentType()) {
                        case Comma: {  // RichYPathTags.KEY_SEPARATOR_TOKEN
                            tokenizer.parseNext();
                            break;
                        }
                        case RightParenthesis: {  // RichYPathTags.END_TUPLE_TOKEN
                            break;
                        }
                        default: {
                            throwUnexpectedToken(tokenizer.getCurrentToken());
                        }
                    }
                }
                rangeLimit = RangeLimit.key(keys);
                tokenizer.parseNext();
                break;
            }
            default: {
                rangeLimit = RangeLimit.key(List.of(parseKeyPart(tokenizer)));
                break;
            }
        }

        tokenizer.getCurrentToken().expectTypes(separators);
        return Optional.of(rangeLimit);
    }

    private static YTreeNode parseKeyPart(YsonTokenizer tokenizer) {
        YTreeNode value = null;
        switch (tokenizer.getCurrentType()) {
            case String: {
                value = YTree.stringNode(tokenizer.getCurrentToken().asStringValue());
                break;
            }
            case Int64: {
                value = YTree.integerNode(tokenizer.getCurrentToken().asInt64Value());
                break;
            }
            case Uint64: {
                value = YTree.unsignedIntegerNode(tokenizer.getCurrentToken().asUint64Value());
                break;
            }
            case Double: {
                value = YTree.doubleNode(tokenizer.getCurrentToken().asDoubleValue());
                break;
            }
            case Boolean: {
                value = YTree.booleanNode(tokenizer.getCurrentToken().asBooleanValue());
                break;
            }
            case Hash: {
                value = YTree.entityNode();
                break;
            }
            default: {
                throwUnexpectedToken(tokenizer.getCurrentToken());
            }
        }

        tokenizer.parseNext();
        return value;
    }

    private static void parseRowRanges(YsonTokenizer tokenizer, Map<String, YTreeNode> attributes) {
        if (tokenizer.getCurrentType() != RichYPathTags.BEGIN_ROW_SELECTOR_TOKEN) {
            return;
        }

        tokenizer.parseNext();

        YTreeBuilder rangesBuilder = YTree.listBuilder();
        boolean finished = false;
        while (!finished) {
            Optional<RangeLimit> lowerLimit = parseRowLimit(
                    tokenizer,
                    List.of(
                            RichYPathTags.RANGE_TOKEN,
                            RichYPathTags.RANGE_SEPARATOR_TOKEN,
                            RichYPathTags.END_ROW_SELECTOR_TOKEN
                    )
            );
            Optional<RangeLimit> upperLimit = Optional.empty();
            boolean isTwoSideRange = false;

            if (tokenizer.getCurrentType() == RichYPathTags.RANGE_TOKEN) {
                isTwoSideRange = true;
                tokenizer.parseNext();
                upperLimit = parseRowLimit(
                        tokenizer, List.of(RichYPathTags.RANGE_SEPARATOR_TOKEN, RichYPathTags.END_ROW_SELECTOR_TOKEN));
            }

            var rangeBuilder = YTree.mapBuilder();
            if (isTwoSideRange) {
                lowerLimit.ifPresent(rangeLimit -> rangeBuilder.key("lower_limit").apply(rangeLimit::toTree));
                upperLimit.ifPresent(rangeLimit -> rangeBuilder.key("upper_limit").apply(rangeLimit::toTree));
            } else {
                lowerLimit.ifPresent(rangeLimit -> rangeBuilder.key("exact").apply(rangeLimit::toTree));
            }
            rangesBuilder.value(rangeBuilder.buildMap());

            if (tokenizer.getCurrentType() == RichYPathTags.END_ROW_SELECTOR_TOKEN) {
                finished = true;
            }
            tokenizer.parseNext();
        }
        attributes.put("ranges", rangesBuilder.buildList());
    }

    private static void parseColumns(YsonTokenizer tokenizer, Map<String, YTreeNode> attributes) {
        if (tokenizer.getCurrentType() != RichYPathTags.BEGIN_COLUMN_SELECTOR_TOKEN) {
            return;
        }

        tokenizer.parseNext();
        YTreeBuilder columnsBuilder = YTree.listBuilder();
        while (tokenizer.getCurrentType() != RichYPathTags.END_COLUMN_SELECTOR_TOKEN) {

            if (tokenizer.getCurrentType() == YsonTokenType.String) {
                columnsBuilder.value(YTree.stringNode(tokenizer.getCurrentToken().asStringValue()));
                tokenizer.parseNext();
            } else {
                throwUnexpectedToken(tokenizer.getCurrentToken());
            }

            switch (tokenizer.getCurrentType()) {
                case Comma: {  // RichYPathTags.COLUMN_SEPARATOR_TOKE
                    tokenizer.parseNext();
                    break;
                }
                case RightBrace: {  // RichYPathTags.END_COLUMN_SELECTOR_TOKE
                    break;
                }
                default: {
                    throwUnexpectedToken(tokenizer.getCurrentToken());
                }
            }
        }
        tokenizer.parseNext();
        attributes.put("columns", columnsBuilder.buildList());
    }

    private static void throwUnexpectedToken(YsonToken tag) {
        throw new RuntimeException("Unexpected token '" + tag + "'");
    }
}
