package tech.ytsaurus.flow.stream;

import java.util.ArrayList;
import java.util.List;

import tech.ytsaurus.flow.row.FlowMessage;

/**
 * Builds typed {@link FlowStream} instances from {@link FlowMessage}-annotated message POJOs.
 */
public final class FlowStreamAnnotations {

    private FlowStreamAnnotations() {
    }

    /**
     * Builds one typed {@link FlowStream} per id declared in the class's {@link FlowMessage}
     * annotation.
     *
     * @param messageClass a message POJO annotated with {@link FlowMessage} (and JPA {@code @Entity})
     * @return one typed stream per id in {@link FlowMessage#streamIds()}
     * @throws IllegalArgumentException if the class is not annotated with {@link FlowMessage} or its
     *                                  {@link FlowMessage#streamIds()} is empty
     */
    public static List<FlowStream<?>> fromAnnotatedClass(Class<?> messageClass) {
        FlowMessage annotation = messageClass.getAnnotation(FlowMessage.class);
        if (annotation == null) {
            throw new IllegalArgumentException(
                    "Class %s is not annotated with @FlowMessage".formatted(messageClass.getName())
            );
        }
        String[] streamIds = annotation.streamIds();
        if (streamIds.length == 0) {
            throw new IllegalArgumentException(
                    "@FlowMessage on %s must declare at least one stream id".formatted(messageClass.getName())
            );
        }
        List<FlowStream<?>> streams = new ArrayList<>(streamIds.length);
        for (String streamId : streamIds) {
            streams.add(FlowStreams.typed(streamId, messageClass));
        }
        return streams;
    }

    /**
     * Flat-maps {@link #fromAnnotatedClass(Class)} over the given message classes.
     *
     * @param messageClasses the message POJOs to translate
     * @return all typed streams declared by the given classes
     * @throws IllegalArgumentException if any class fails the {@link #fromAnnotatedClass(Class)}
     *                                  preconditions
     */
    public static List<FlowStream<?>> fromAnnotatedClasses(Iterable<Class<?>> messageClasses) {
        List<FlowStream<?>> streams = new ArrayList<>();
        for (Class<?> messageClass : messageClasses) {
            streams.addAll(fromAnnotatedClass(messageClass));
        }
        return streams;
    }
}
