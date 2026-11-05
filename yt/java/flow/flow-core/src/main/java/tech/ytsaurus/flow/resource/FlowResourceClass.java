package tech.ytsaurus.flow.resource;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Optional spec-facing implementation type name for class-based resource factory registration.
 *
 * <p>A resource class registered by class is named by its fully-qualified class name by default;
 * this annotation replaces that name with a value decoupled from the type name, keeping deployed
 * pipeline specs stable across renames and repackagings.
 *
 * <p>This is not the resource id from {@code spec.resources} or a computation's resource alias.
 * It does not register the class automatically or make it a Spring bean. Explicit string-based
 * registration and Spring {@code ResourceProvider} maps use their supplied names instead.
 *
 * @see tech.ytsaurus.flow.context.PipelineContext#registerResourceClass(Class, java.util.function.Supplier)
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface FlowResourceClass {

    /**
     * The name the pipeline spec uses under the {@code companion_resource_class} parameter of a
     * resource spec; empty means the fully-qualified class name.
     *
     * @return the spec-facing resource class name.
     */
    String value() default "";
}
