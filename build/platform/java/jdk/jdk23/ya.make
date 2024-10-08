RESOURCES_LIBRARY()

DECLARE_EXTERNAL_HOST_RESOURCES_BUNDLE_BY_JSON(JDK23 jdk.json)
SET_RESOURCE_URI_FROM_JSON(WITH_JDK23_URI jdk.json)

IF (WITH_JDK23_URI)
    DECLARE_EXTERNAL_RESOURCE(WITH_JDK23 ${WITH_JDK23_URI})
ENDIF()

END()

IF (AUTOCHECK)
    RECURSE_FOR_TESTS(ut)
ENDIF()
IF(YA_IDE_IDEA)
    RECURSE_FOR_TESTS(ut)
ENDIF()
