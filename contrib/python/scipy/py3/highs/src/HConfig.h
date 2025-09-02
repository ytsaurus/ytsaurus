#ifndef HCONFIG_H_
#define HCONFIG_H_

#undef FAST_BUILD
#undef ZLIB_FOUND
/* #undef CUPDLP_CPU */
#undef HIGHSINT64
#if defined(__x86_64__)
#define HIGHS_HAVE_MM_PAUSE
#else
#undef HIGHS_HAVE_MM_PAUSE
#endif
#define HIGHS_HAVE_BUILTIN_CLZ
/* #undef HIGHS_HAVE_BITSCAN_REVERSE */
/* #undef HIGHS_NO_DEFAULT_THREADS */

#define HIGHS_GITHASH "unknown"
#define HIGHS_VERSION_MAJOR 1
#define HIGHS_VERSION_MINOR 8
#define HIGHS_VERSION_PATCH 0

#endif /* HCONFIG_H_ */
