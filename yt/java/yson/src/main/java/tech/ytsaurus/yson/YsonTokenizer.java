package tech.ytsaurus.yson;

public class YsonTokenizer {
    private YsonToken token = YsonToken.startOfStream();
    private final YsonLexer lexer;

    public YsonTokenizer(byte[] buffer) {
        this.lexer = new YsonLexer(buffer);
    }

    public void parseNext() {
        token = lexer.getNextToken();
    }

    public YsonToken getCurrentToken() {
        return token;
    }

    public YsonTokenType getCurrentType() {
        return getCurrentToken().getType();
    }
}
