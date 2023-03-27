package tech.ytsaurus.client.rows;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static tech.ytsaurus.core.utils.ClassUtils.getValueOfAnnotationProperty;

class JavaPersistenceApi {
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
    private static final Set<String> ENTITY_ANNOTATIONS = getAnnotationsFor(ENTITY);
    private static final Set<String> TRANSIENT_ANNOTATIONS = getAnnotationsFor(TRANSIENT);
    private static final Set<String> COLUMN_ANNOTATIONS = getAnnotationsFor(COLUMN);

    private JavaPersistenceApi() {
    }

    static Set<String> transientAnnotations() {
        return TRANSIENT_ANNOTATIONS;
    }

    static Set<String> entityAnnotations() {
        return ENTITY_ANNOTATIONS;
    }

    static Set<String> columnAnnotations() {
        return COLUMN_ANNOTATIONS;
    }

    static String getEntityName(Annotation entityAnnotation) {
        return getValueOfAnnotationProperty(entityAnnotation, ENTITY_NAME);
    }

    static String getColumnName(Annotation columnAnnotation) {
        return getValueOfAnnotationProperty(columnAnnotation, COLUMN_NAME);
    }

    static boolean isColumnNullable(Annotation columnAnnotation) {
        return getValueOfAnnotationProperty(columnAnnotation, COLUMN_NULLABLE);
    }

    static int getColumnPrecision(Annotation columnAnnotation) {
        return getValueOfAnnotationProperty(columnAnnotation, COLUMN_PRECISION);
    }

    static int getColumnScale(Annotation columnAnnotation) {
        return getValueOfAnnotationProperty(columnAnnotation, COLUMN_SCALE);
    }

    private static Set<String> getAnnotationsFor(String annotationName) {
        return PACKAGES.stream()
                .map(pack -> pack + "." + annotationName)
                .collect(Collectors.toUnmodifiableSet());
    }
}
