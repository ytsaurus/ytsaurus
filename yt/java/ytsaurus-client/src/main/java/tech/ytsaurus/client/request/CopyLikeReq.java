package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTreeBuilder;


public abstract class CopyLikeReq<
        TBuilder extends CopyLikeReq.Builder<TBuilder, TRequest>,
        TRequest extends CopyLikeReq<TBuilder, TRequest>>
        extends MutateNode<TBuilder, TRequest> {

    protected final String source;
    protected final String destination;

    protected final boolean recursive;
    protected final boolean force;
    protected final boolean preserveAccount;
    protected final boolean preserveExpirationTime;
    protected final boolean preserveCreationTime;
    protected final boolean ignoreExisting;

    protected CopyLikeReq(Builder<?, ?> builder) {
        super(builder);
        source = Objects.requireNonNull(builder.source);
        destination = Objects.requireNonNull(builder.destination);
        recursive = builder.recursive;
        force = builder.force;
        preserveAccount = builder.preserveAccount;
        preserveExpirationTime = builder.preserveExpirationTime;
        preserveCreationTime = builder.preserveCreationTime;
        ignoreExisting = builder.ignoreExisting;
    }

    public YPath getSource() {
        return YPath.simple(source);
    }

    public YPath getDestination() {
        return YPath.simple(destination);
    }

    public boolean getRecursive() {
        return recursive;
    }

    public boolean getForce() {
        return force;
    }

    public boolean getPreserveAccount() {
        return preserveAccount;
    }

    public boolean getPreserveExpirationTime() {
        return preserveExpirationTime;
    }

    public boolean getPreserveCreationTime() {
        return preserveCreationTime;
    }

    public boolean getIgnoreExisting() {
        return ignoreExisting;
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("Source: ").append(source).append("; Destination: ").append(destination).append("; ");
        if (recursive) {
            sb.append("Recursive: true; ");
        }
        if (ignoreExisting) {
            sb.append("IgnoreExisting: true; ");
        }
        if (force) {
            sb.append("Force: true; ");
        }
        if (preserveAccount) {
            sb.append("PreserveAccount: true; ");
        }
        if (preserveCreationTime) {
            sb.append("PreserveCreationTime: true; ");
        }
        if (preserveExpirationTime) {
            sb.append("PreserveExpirationTime: true; ");
        }
        super.writeArgumentsLogString(sb);
    }

    @Override
    public YTreeBuilder toTree(YTreeBuilder builder) {
        return toTree(builder, "source_path", "destination_path");
    }

    public YTreeBuilder toTree(YTreeBuilder builder, String sourcePathKey, String destinationPathKey) {
        return builder
                .apply(super::toTree)
                .key(sourcePathKey).value(source)
                .key(destinationPathKey).value(destination)
                .key("recursive").value(recursive)
                .key("force").value(force)
                .key("preserve_account").value(preserveAccount)
                .key("preserve_expiration_time").value(preserveExpirationTime)
                .key("preserve_creation_time").value(preserveCreationTime)
                .key("ignore_existing").value(ignoreExisting);
    }

    public abstract static class Builder<
            TBuilder extends Builder<TBuilder, TRequest>,
            TRequest extends MutateNode<?, TRequest>>
            extends MutateNode.Builder<TBuilder, TRequest> {
        @Nullable
        protected String source;
        @Nullable
        protected String destination;

        protected boolean recursive = false;
        protected boolean force = false;
        protected boolean preserveAccount = false;
        protected boolean preserveExpirationTime = false;
        protected boolean preserveCreationTime = false;
        protected boolean ignoreExisting = false;

        protected Builder() {
        }

        protected Builder(Builder<?, ?> builder) {
            super(builder);
            this.source = builder.source;
            this.destination = builder.destination;
            this.recursive = builder.recursive;
            this.force = builder.force;
            this.preserveAccount = builder.preserveAccount;
            this.preserveCreationTime = builder.preserveCreationTime;
            this.preserveExpirationTime = builder.preserveExpirationTime;
            this.ignoreExisting = builder.ignoreExisting;
        }

        public TBuilder setSource(String source) {
            this.source = source;
            return self();
        }

        public TBuilder setDestination(String destination) {
            this.destination = destination;
            return self();
        }

        public TBuilder setRecursive(boolean recursive) {
            this.recursive = recursive;
            return self();
        }

        public TBuilder setForce(boolean force) {
            this.force = force;
            return self();
        }

        public TBuilder setPreserveAccount(boolean preserveAccount) {
            this.preserveAccount = preserveAccount;
            return self();
        }

        public TBuilder setPreserveExpirationTime(boolean preserveExpirationTime) {
            this.preserveExpirationTime = preserveExpirationTime;
            return self();
        }

        public TBuilder setPreserveCreationTime(boolean preserveCreationTime) {
            this.preserveCreationTime = preserveCreationTime;
            return self();
        }

        public TBuilder setIgnoreExisting(boolean ignoreExisting) {
            this.ignoreExisting = ignoreExisting;
            return self();
        }

        public YPath getSource() {
            return YPath.simple(source);
        }

        public YPath getDestination() {
            return YPath.simple(destination);
        }

        public boolean getRecursive() {
            return recursive;
        }

        public boolean getForce() {
            return force;
        }

        public boolean getPreserveAccount() {
            return preserveAccount;
        }

        public boolean getPreserveExpirationTime() {
            return preserveExpirationTime;
        }

        public boolean getPreserveCreationTime() {
            return preserveCreationTime;
        }

        public boolean getIgnoreExisting() {
            return ignoreExisting;
        }

        @Override
        public YTreeBuilder toTree(YTreeBuilder builder) {
            return toTree(builder, "source_path", "destination_path");
        }

        public YTreeBuilder toTree(YTreeBuilder builder, String sourcePathKey, String destinationPathKey) {
            return builder
                    .apply(super::toTree)
                    .key(sourcePathKey).value(source)
                    .key(destinationPathKey).value(destination)
                    .key("recursive").value(recursive)
                    .key("force").value(force)
                    .key("preserve_account").value(preserveAccount)
                    .key("preserve_expiration_time").value(preserveExpirationTime)
                    .key("preserve_creation_time").value(preserveCreationTime)
                    .key("ignore_existing").value(ignoreExisting);
        }

        @Override
        protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
            sb.append("Source: ").append(source).append("; Destination: ").append(destination).append("; ");
            if (recursive) {
                sb.append("Recursive: true; ");
            }
            if (ignoreExisting) {
                sb.append("IgnoreExisting: true; ");
            }
            if (force) {
                sb.append("Force: true; ");
            }
            if (preserveAccount) {
                sb.append("PreserveAccount: true; ");
            }
            if (preserveCreationTime) {
                sb.append("PreserveCreationTime: true; ");
            }
            if (preserveExpirationTime) {
                sb.append("PreserveExpirationTime: true; ");
            }
            super.writeArgumentsLogString(sb);
        }
    }
}
