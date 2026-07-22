package tech.ytsaurus.flow.service;

import java.time.Duration;

import tech.ytsaurus.core.DataSize;

public class ResourceStats {
    private final DataSize allocatedBytes;
    private final Duration cpuTime;

    public ResourceStats(DataSize allocatedBytes, Duration cpuTime) {
        this.allocatedBytes = allocatedBytes;
        this.cpuTime = cpuTime;
    }

    public DataSize getAllocatedBytes() {
        return allocatedBytes;
    }

    public Duration getCpuTime() {
        return cpuTime;
    }

    @Override
    public String toString() {
        return "ResourceStats{" +
                "allocatedBytes=" + allocatedBytes +
                ", cpuTime=" + cpuTime +
                '}';
    }
}
