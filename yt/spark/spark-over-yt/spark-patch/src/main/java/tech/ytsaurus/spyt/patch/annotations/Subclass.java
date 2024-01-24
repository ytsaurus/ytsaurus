package tech.ytsaurus.spyt.patch.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.CLASS)
@Target(ElementType.TYPE)
public @interface Subclass {
    /**
     * Base class that this subclass will substitute.
     */
    String value();
}
