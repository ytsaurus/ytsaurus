package tech.ytsaurus.client.operations;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

/**
 * Immutable disk request for a user job.
 */
@NonNullApi
@NonNullFields
public class DiskRequest {
    @Nullable
    private final DataSize diskSpace;
    @Nullable
    private final Long inodeCount;
    @Nullable
    private final String account;
    @Nullable
    private final String mediumName;

    /**
     * Create disk request with required disk space and default medium.
     */
    public DiskRequest(DataSize diskSpace) {
        this(builder().setDiskSpace(diskSpace));
    }

    protected <T extends BuilderBase<T>> DiskRequest(BuilderBase<T> builder) {
        diskSpace = builder.diskSpace;
        inodeCount = builder.inodeCount;
        account = builder.account;
        mediumName = builder.mediumName;
    }

    /**
     * @see BuilderBase#setDiskSpace(DataSize)
     */
    public Optional<DataSize> getDiskSpace() {
        return Optional.ofNullable(diskSpace);
    }

    /**
     * @see BuilderBase#setInodeCount(Long)
     */
    public Optional<Long> getInodeCount() {
        return Optional.ofNullable(inodeCount);
    }

    /**
     * @see BuilderBase#setAccount(String)
     */
    public Optional<String> getAccount() {
        return Optional.ofNullable(account);
    }

    /**
     * @see BuilderBase#setMediumName(String)
     */
    public Optional<String> getMediumName() {
        return Optional.ofNullable(mediumName);
    }

    /**
     * Convert disk request to yson.
     */
    public YTreeMapNode prepare() {
        return YTree.mapBuilder()
                .when(diskSpace != null, b -> b.key("disk_space").value(Objects.requireNonNull(diskSpace).toBytes()))
                .when(inodeCount != null, b -> b.key("inode_count").value(inodeCount))
                .when(account != null, b -> b.key("account").value(account))
                .when(mediumName != null, b -> b.key("medium_name").value(mediumName))
                .buildMap();
    }

    @Override
    public int hashCode() {
        return Objects.hash(diskSpace == null ? null : diskSpace.toBytes(), inodeCount, account, mediumName);
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        DiskRequest request = (DiskRequest) obj;
        return Objects.equals(
                diskSpace == null ? null : diskSpace.toBytes(),
                request.diskSpace == null ? null : request.diskSpace.toBytes())
                && Objects.equals(inodeCount, request.inodeCount)
                && Objects.equals(account, request.account)
                && Objects.equals(mediumName, request.mediumName);
    }

    public static BuilderBase<?> builder() {
        return new Builder();
    }

    /**
     * Builder of {@link DiskRequest}.
     */
    protected static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    // BuilderBase was taken out because there is another client
    // which we need to support too and which use the same DiskRequest class.
    @NonNullApi
    @NonNullFields
    public abstract static class BuilderBase<T extends BuilderBase<T>> {
        @Nullable
        private DataSize diskSpace;
        @Nullable
        private Long inodeCount;
        @Nullable
        private String account;
        @Nullable
        private String mediumName;

        /**
         * Create instance of {@link DiskRequest}.
         */
        public DiskRequest build() {
            if (diskSpace == null) {
                throw new IllegalStateException("\"disk_space\" is required in disk request");
            }
            if (account != null && mediumName == null) {
                throw new IllegalStateException(
                        "\"medium_name\" is required in disk request if \"account\" is specified");
            }
            return new DiskRequest(this);
        }

        /**
         * Set required disk space in bytes.
         */
        public T setDiskSpace(@Nullable DataSize diskSpace) {
            this.diskSpace = diskSpace;
            return self();
        }

        /**
         * Set inode count limit.
         */
        public T setInodeCount(@Nullable Long inodeCount) {
            this.inodeCount = inodeCount;
            return self();
        }

        /**
         * Set account whose disk quota will be used.
         */
        public T setAccount(@Nullable String account) {
            this.account = account;
            return self();
        }

        /**
         * Set medium name corresponding to required disk type.
         */
        public T setMediumName(@Nullable String mediumName) {
            this.mediumName = mediumName;
            return self();
        }

        protected abstract T self();
    }
}
