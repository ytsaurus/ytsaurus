package org.apache.livy.rsc.driver;

import java.io.Serializable;

public final class QueryPlan implements Serializable {
    public final String dot;

    public final String[] meta;

    public QueryPlan(String dot, String[] meta) {
        this.dot = dot;
        this.meta = meta;
    }
}
