package tech.ytsaurus.yson;

public enum YsonTokenType {
    StartOfStream,
    EndOfStream,
    String,
    Int64,
    Uint64,
    Double,
    Boolean,
    // Special values:
    // YSON
    Semicolon, // ;
    Equals,  // =
    Hash,  // #
    LeftBracket, // [
    RightBracket,  // ]
    LeftBrace,  // {
    RightBrace,  // }
    LeftAngle,  // <
    RightAngle,  // >
    // Table ranges
    LeftParenthesis, // (
    RightParenthesis, // )
    Plus, // +
    Colon, // :
    Comma, // ,
    // YPath
    Slash; // /

    public static YsonTokenType fromSymbol(byte ch) {
        switch (ch) {
            case ';': return YsonTokenType.Semicolon;
            case '=': return YsonTokenType.Equals;
            case '#': return YsonTokenType.Hash;
            case '[': return YsonTokenType.LeftBracket;
            case ']': return YsonTokenType.RightBracket;
            case '{': return YsonTokenType.LeftBrace;
            case '}': return YsonTokenType.RightBrace;
            case '<': return YsonTokenType.LeftAngle;
            case '>': return YsonTokenType.RightAngle;
            case '(': return YsonTokenType.LeftParenthesis;
            case ')': return YsonTokenType.RightParenthesis;
            case '+': return YsonTokenType.Plus;
            case ':': return YsonTokenType.Colon;
            case ',': return YsonTokenType.Comma;
            case '/': return YsonTokenType.Slash;
            default: return YsonTokenType.EndOfStream;
        }
    }
}
