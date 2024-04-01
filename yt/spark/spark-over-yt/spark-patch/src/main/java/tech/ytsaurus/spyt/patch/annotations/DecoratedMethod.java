package tech.ytsaurus.spyt.patch.annotations;

import tech.ytsaurus.spyt.patch.MethodProcesor;

public @interface DecoratedMethod {
    String name() default "";
    String signature() default "";
    Class<? extends MethodProcesor>[] baseMethodProcessors() default {};
}
