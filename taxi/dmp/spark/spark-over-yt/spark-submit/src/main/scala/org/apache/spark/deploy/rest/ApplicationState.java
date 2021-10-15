package org.apache.spark.deploy.rest;

public enum ApplicationState {
    WAITING, RUNNING, FINISHED, FAILED, KILLED, UNKNOWN, UNDEFINED
}
