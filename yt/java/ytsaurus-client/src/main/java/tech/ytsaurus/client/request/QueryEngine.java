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

    @Override
    public String toString() {
        return stringValue;
    }

    EQueryEngine getProtoValue() {
        return protoValue;
    }
}
