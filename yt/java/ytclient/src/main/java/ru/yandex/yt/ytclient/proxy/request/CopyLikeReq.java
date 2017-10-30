package ru.yandex.yt.ytclient.proxy.request;

public abstract class CopyLikeReq<T extends CopyLikeReq> extends MutateNode<T> {

    protected final String from;
    protected final String to;

    protected boolean recursive = false;
    protected boolean force = false;
    protected boolean preserveAccount = false;
    protected boolean preserveExpirationTime = false;
    protected boolean preserveCreationTime = false;
    protected boolean ignoreExisting = false;

    public CopyLikeReq(String from, String to) {
        this.from = from;
        this.to = to;
    }

    public T setRecursive(boolean recursive) {
        this.recursive = recursive;
        return (T)this;
    }

    public T setForce(boolean f) {
        this.force = f;
        return (T)this;
    }

    public T setPreserveAccount(boolean f) {
        this.preserveAccount = f;
        return (T)this;
    }

    public T setPreserveExpirationTime(boolean f) {
        this.preserveExpirationTime = f;
        return (T)this;
    }

    public T setPreserveCreationTime(boolean f) {
        this.preserveCreationTime = f;
        return (T)this;
    }

    public T setIgnoreExisting(boolean f) {
        this.ignoreExisting = f;
        return (T)this;
    }
}
