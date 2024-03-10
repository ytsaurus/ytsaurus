package tech.ytsaurus.spyt.patch.annotations;

public @interface DecoratedMethod {
    String name() default "";
    String signature() default "";
}
