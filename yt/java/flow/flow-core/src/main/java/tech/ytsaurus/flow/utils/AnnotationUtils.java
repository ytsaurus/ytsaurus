package tech.ytsaurus.flow.utils;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;

import static tech.ytsaurus.core.utils.ClassUtils.anyMatchWithAnnotation;
import static tech.ytsaurus.core.utils.ClassUtils.getValueOfAnnotationProperty;

/**
 * Fork of tech.ytsaurus.client.rows.JavaPersistenceApi with public modifiers.
 */
public class AnnotationUtils {
    private static final List<String> PACKAGES = List.of(
            "javax.persistence", "jakarta.persistence"
    );
    private static final String ENTITY = "Entity";
    private static final String ENTITY_NAME = "name";
    private static final String TRANSIENT = "Transient";
    private static final String COLUMN = "Column";
    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_NULLABLE = "nullable";
    private static final String COLUMN_PRECISION = "precision";
    private static final String COLUMN_SCALE = "scale";
    private static final String COLUMN_DEFINITION = "columnDefinition";
    private static final String EMBEDDABLE = "Embeddable";
    private static final String EMBEDDED = "Embedded";
    private static final Set<String> ENTITY_ANNOTATIONS = getAnnotationsFor(ENTITY);
    private static final Set<String> TRANSIENT_ANNOTATIONS = getAnnotationsFor(TRANSIENT);
    private static final Set<String> COLUMN_ANNOTATIONS = getAnnotationsFor(COLUMN);
    private static final Set<String> EMBEDDABLE_ANNOTATIONS = getAnnotationsFor(EMBEDDABLE);
    private static final Set<String> EMBEDDED_ANNOTATIONS = getAnnotationsFor(EMBEDDED);

    private AnnotationUtils() {
    }

    public static Set<String> transientAnnotations() {
        return TRANSIENT_ANNOTATIONS;
    }

    public static Set<String> entityAnnotations() {
        return ENTITY_ANNOTATIONS;
    }

    public static Set<String> columnAnnotations() {
        return COLUMN_ANNOTATIONS;
    }

    public static Set<String> embeddableAnnotations() {
        return EMBEDDABLE_ANNOTATIONS;
    }

    public static Set<String> embeddedAnnotations() {
        return EMBEDDED_ANNOTATIONS;
    }

    public static boolean isColumnAnnotationPresent(@Nullable Annotation annotation) {
        return annotation != null &&
                anyMatchWithAnnotation(annotation, columnAnnotations());
    }

    public static String getEntityName(Annotation entityAnnotation) {
        return getValueOfAnnotationProperty(entityAnnotation, ENTITY_NAME);
    }

    public static String getColumnName(Annotation columnAnnotation) {
        return getValueOfAnnotationProperty(columnAnnotation, COLUMN_NAME);
    }

    public static boolean isColumnNullable(Annotation columnAnnotation) {
        return getValueOfAnnotationProperty(columnAnnotation, COLUMN_NULLABLE);
    }

    public static int getColumnPrecision(Annotation columnAnnotation) {
        return getValueOfAnnotationProperty(columnAnnotation, COLUMN_PRECISION);
    }

    public static int getColumnScale(Annotation columnAnnotation) {
        return getValueOfAnnotationProperty(columnAnnotation, COLUMN_SCALE);
    }

    public static String getColumnDefinition(Annotation columnAnnotation) {
        return getValueOfAnnotationProperty(columnAnnotation, COLUMN_DEFINITION);
    }

    private static Set<String> getAnnotationsFor(String annotationName) {
        return PACKAGES.stream()
                .map(pack -> pack + "." + annotationName)
                .collect(Collectors.toUnmodifiableSet());
    }
}
