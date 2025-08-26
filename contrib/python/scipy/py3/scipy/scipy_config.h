/* Thread-local storage */
/* #undef HAVE_THREAD_LOCAL */
#define HAVE__THREAD_LOCAL 1
#define HAVE__THREAD 1
/* #undef HAVE___DECLSPEC_THREAD_ */

#ifdef __cplusplus
    #define SCIPY_TLS thread_local
#elif defined(HAVE_THREAD_LOCAL)
    #define SCIPY_TLS thread_local
#elif defined(HAVE__THREAD_LOCAL)
    #define SCIPY_TLS _Thread_local
#elif defined(HAVE___THREAD)
    #define SCIPY_TLS __thread
#elif defined(HAVE___DECLSPEC_THREAD_)
    #define SCIPY_TLS __declspec(thread)
#else
    #define SCIPY_TLS
#endif
