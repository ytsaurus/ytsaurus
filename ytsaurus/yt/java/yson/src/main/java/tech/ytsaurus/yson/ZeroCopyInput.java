package tech.ytsaurus.yson;

public interface ZeroCopyInput {
    boolean next(BufferReference out);
}
