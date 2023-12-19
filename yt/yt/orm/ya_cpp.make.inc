# Common defaults for C++ projects in ORM.
# Make a transitive include in your ORM project and include that in all |PROGRAM|,
# |GTEST| and |UNITTEST| projects. Override settings as needed.

# TCMalloc performs as well as the YT allocator while also providing a memory profiler.
# If there's a collision over |abseil|, just switch that project to the |YT| alocator.
IF (OS_LINUX)
    ALLOCATOR(TCMALLOC)
ENDIF()
