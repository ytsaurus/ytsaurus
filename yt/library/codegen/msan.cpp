#include "msan.h"

#if defined(_msan_enabled_)

#define XX(msan_tls_variable) \
extern "C" __thread int msan_tls_variable; \
void yt_ ## msan_tls_variable(void** p) __attribute__((no_sanitize("memory"))) \
{ \
    *p = (void*)&msan_tls_variable; \
}

MSAN_TLS_STUBS

#undef XX

#endif
