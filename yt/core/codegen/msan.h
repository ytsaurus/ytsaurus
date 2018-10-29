#pragma once

#include <util/system/sanitizers.h>

#if defined(_msan_enabled_)

#define MSAN_TLS_STUBS    \
    XX(__msan_origin_tls) \
    XX(__msan_param_origin_tls) \
    XX(__msan_param_tls) \
    XX(__msan_retval_origin_tls) \
    XX(__msan_retval_tls) \
    XX(__msan_va_arg_overflow_size_tls) \
    XX(__msan_va_arg_tls)

#define XX(msan_tls_variable) \
void yt_ ## msan_tls_variable(void**);

MSAN_TLS_STUBS

#undef XX

#endif
