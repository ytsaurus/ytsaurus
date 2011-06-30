#include <string.h>

void *_intel_fast_memset(void *s, int c, size_t n) {
    return memset(s, c, n);
}

void *_intel_fast_memcpy(void *dest, const void *src, size_t n) {
    return memcpy(dest, src, n);
}

int _intel_fast_memcmp(const void *s1, const void *s2, size_t n) {
    return memcmp(s1, s2, n);
}

void __intel_new_proc_init() {
}
