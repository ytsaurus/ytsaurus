package org.apache.spark.deploy.rest;

// copy of org.apache.spark.deploy.master.DriverState
public enum DriverState {
    SUBMITTED, RUNNING, FINISHED, RELAUNCHING, UNKNOWN, KILLED, FAILED, ERROR;
}
