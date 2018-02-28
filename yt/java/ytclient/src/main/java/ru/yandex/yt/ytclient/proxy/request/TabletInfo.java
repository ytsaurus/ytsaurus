package ru.yandex.yt.ytclient.proxy.request;

public class TabletInfo {
    private final long totalRowCount;
    private final long trimmedRowCount;

    public TabletInfo(long totalRowCount, long trimmedRowCount)
    {
        this.totalRowCount = totalRowCount;
        this.trimmedRowCount = trimmedRowCount;
    }

    public long getTotalRowCount() {
        return totalRowCount;
    }

    public long getTrimmedRowCount() {
        return trimmedRowCount;
    }
}
