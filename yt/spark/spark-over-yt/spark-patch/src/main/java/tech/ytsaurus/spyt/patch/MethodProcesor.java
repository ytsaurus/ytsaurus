package tech.ytsaurus.spyt.patch;

import javassist.CtMethod;

public interface MethodProcesor {
    void process(CtMethod method);
}
