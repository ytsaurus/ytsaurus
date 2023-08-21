package tech.ytsaurus.client.operations;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Store all information about failed job.
 */
@NonNullApi
@NonNullFields
public class FailedJobInfo {
    private final GUID jobId;
    private final List<String> errorMessages = new ArrayList<>();
    @Nullable
    private String stderr;

    public FailedJobInfo(GUID jobId) {
        this.jobId = jobId;
    }

    public GUID getJobId() {
        return jobId;
    }

    public List<String> getErrorMessages() {
        return errorMessages;
    }

    public Optional<String> getStderr() {
        return Optional.ofNullable(stderr);
    }

    public void setStderr(String stderr) {
        this.stderr = stderr;
    }

    public void addErrorMessage(String errorMessage) {
        errorMessages.add(errorMessage);
    }

    @Override
    public String toString() {
        String errorMessagesStr = errorMessages.stream()
                .map(e -> "- " + e)
                .collect(Collectors.joining("\n"));
        return "Job id: " + jobId +
                "\nError message:\n" + errorMessagesStr +
                "\nJob stderr: " + stderr;
    }

}
