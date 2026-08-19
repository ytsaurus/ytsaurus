package tech.ytsaurus.flow.row;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the typed Flow streams that carry a message POJO.
 * <p>
 * Used together with the JPA {@code @Entity} annotation, from which the stream schema is derived;
 * {@code @FlowMessage} only names the stream ids.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface FlowMessage {

    /**
     * The stream ids carrying this message type: at least one, each unique across the pipeline and
     * matching a stream in the static spec.
     *
     * @return the stream ids.
     */
    String[] streamIds();
}
