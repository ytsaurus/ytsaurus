package tech.ytsaurus.client.request;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;


public class GetJobStderrResult {
    @Nullable
    private final byte[] stderr;

    public GetJobStderrResult(List<byte[]> attachments) {
        if (attachments.isEmpty()) {
            stderr = null;
        } else {
            stderr = attachments.get(0);
        }
    }

    public Optional<byte[]> getStderr() {
        return Optional.ofNullable(stderr);
    }
}
