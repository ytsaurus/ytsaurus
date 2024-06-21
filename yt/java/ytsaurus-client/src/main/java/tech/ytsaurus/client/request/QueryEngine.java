package tech.ytsaurus.client.request;

import tech.ytsaurus.rpcproxy.EQueryEngine;

public enum QueryEngine {
    Ql(EQueryEngine.QE_QL, "ql"),
    Yql(EQueryEngine.QE_YQL, "yql"),
    Chyt(EQueryEngine.QE_CHYT, "chyt"),
    Mock(EQueryEngine.QE_MOCK, "mock"),
    Spyt(EQueryEngine.QE_SPYT, "spyt");

    private final EQueryEngine protoValue;
    private final String stringValue;

    QueryEngine(EQueryEngine protoValue, String stringValue) {
        this.protoValue = protoValue;
        this.stringValue = stringValue;
    }

    public static QueryEngine fromProtoValue(EQueryEngine protoValue) {
        switch (protoValue) {
            case QE_QL:
                return Ql;
            case QE_YQL:
                return Yql;
            case QE_CHYT:
                return Chyt;
            case QE_MOCK:
                return Mock;
            case QE_SPYT:
                return Spyt;
            default:
                throw new IllegalArgumentException("Illegal query engine value " + protoValue);
        }
    }

    @Override
    public String toString() {
        return stringValue;
    }

    EQueryEngine getProtoValue() {
        return protoValue;
    }
}
