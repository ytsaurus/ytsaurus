#pragma once

#if defined(_MSC_VER)
#define OVERRIDE override
#elif defined(__clang__)
#define OVERRIDE __attribute__((override))
#else
#define OVERRIDE
#endif

