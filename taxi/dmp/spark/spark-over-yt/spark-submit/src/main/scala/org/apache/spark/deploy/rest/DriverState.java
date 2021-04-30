package org.apache.spark.deploy.rest;

import java.util.HashSet;
import java.util.Set;

// copy of org.apache.spark.deploy.master.DriverState
public enum DriverState {
    SUBMITTED, RUNNING, FINISHED, RELAUNCHING, UNKNOWN, KILLED, FAILED, ERROR;

    private static Set<DriverState> failedStates = new HashSet<>() {{
        add(UNKNOWN);
        add(KILLED);
        add(FAILED);
        add(ERROR);
    }};

    private static Set<DriverState> successStates = new HashSet<>() {{
        add(FINISHED);
    }};


    public boolean isFinal() {
        return failedStates.contains(this) || successStates.contains(this);
    }

    public boolean isSuccess() {
        return successStates.contains(this);
    }

    public boolean isFailure() {
        return failedStates.contains(this);
    }
}
