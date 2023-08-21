package tech.ytsaurus.core.cypress;

import tech.ytsaurus.yson.YsonTokenType;

class RichYPathTags {
    static final YsonTokenType BEGIN_COLUMN_SELECTOR_TOKEN = YsonTokenType.LeftBrace;
    static final YsonTokenType END_COLUMN_SELECTOR_TOKEN = YsonTokenType.RightBrace;
    static final YsonTokenType COLUMN_SEPARATOR_TOKEN = YsonTokenType.Comma;
    static final YsonTokenType BEGIN_ROW_SELECTOR_TOKEN = YsonTokenType.LeftBracket;
    static final YsonTokenType END_ROW_SELECTOR_TOKEN = YsonTokenType.RightBracket;
    static final YsonTokenType ROW_INDEX_MARKER_TOKEN = YsonTokenType.Hash;
    static final YsonTokenType BEGIN_TUPLE_TOKEN = YsonTokenType.LeftParenthesis;
    static final YsonTokenType END_TUPLE_TOKEN = YsonTokenType.RightParenthesis;
    static final YsonTokenType KEY_SEPARATOR_TOKEN = YsonTokenType.Comma;
    static final YsonTokenType RANGE_TOKEN = YsonTokenType.Colon;
    static final YsonTokenType RANGE_SEPARATOR_TOKEN = YsonTokenType.Comma;

    private RichYPathTags() {
    }
}
