package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;

public abstract class CopyLikeReq<T extends CopyLikeReq<T>> extends MutateNode<T> {

    protected final String source;
    protected final String destination;

    protected boolean recursive = false;
    protected boolean force = false;
    protected boolean preserveAccount = false;
    protected boolean preserveExpirationTime = false;
    protected boolean preserveCreationTime = false;
    protected boolean ignoreExisting = false;

    public CopyLikeReq(String source, String destination) {
        this.source = source;
        this.destination = destination;
    }

    public CopyLikeReq(CopyLikeReq<?> copyLikeReq) {
        super(copyLikeReq);
        source = copyLikeReq.source;
        destination = copyLikeReq.destination;
        recursive = copyLikeReq.recursive;
        preserveAccount = copyLikeReq.preserveAccount;
        preserveExpirationTime = copyLikeReq.preserveExpirationTime;
        preserveCreationTime = copyLikeReq.preserveCreationTime;
        ignoreExisting = copyLikeReq.ignoreExisting;
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

    public T setRecursive(boolean recursive) {
        this.recursive = recursive;
        return self();
    }

    public boolean getForce(boolean force) {
        return force;
    }

    public T setForce(boolean f) {
        this.force = f;
        return self();
    }

    public boolean getPreserveAccount() {
        return preserveAccount;
    }

    public T setPreserveAccount(boolean f) {
        this.preserveAccount = f;
        return self();
    }

    public boolean getPreserveExpirationTime() {
        return preserveExpirationTime;
    }

    public T setPreserveExpirationTime(boolean f) {
        this.preserveExpirationTime = f;
        return self();
    }

    public T setPreserveCreationTime(boolean f) {
        this.preserveCreationTime = f;
        return self();
    }

    public boolean getPreserveCreationTime() {
        return preserveCreationTime;
    }

    public T setIgnoreExisting(boolean f) {
        this.ignoreExisting = f;
        return self();
    }

    public boolean getIgnoreExisting() {
        return ignoreExisting;
    }

    public YTreeBuilder toTree(YTreeBuilder builder) {
        return builder
                .apply(super::toTree)
                .key("source_path").value(source)
                .key("destination_path").value(destination)
                .key("recursive").value(recursive)
                .key("force").value(force)
                .key("preserve_account").value(preserveAccount)
                .key("preserve_expiration_time").value(preserveExpirationTime)
                .key("preserve_creation_time").value(preserveCreationTime)
                .key("ignore_existing").value(ignoreExisting)
                ;
    }
}
