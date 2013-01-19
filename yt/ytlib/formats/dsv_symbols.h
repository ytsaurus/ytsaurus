#pragma once

#include "public.h"

// XXX(sandello): Define this to enable SSE4.2-baked symbol lookup.
#define _YT_USE_SSE42_FOR_DSV_

#ifdef _YT_USE_SSE42_FOR_DSV_
#include <nmmintrin.h>
#endif

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

struct TDsvSymbolTable
{
    explicit TDsvSymbolTable(TDsvFormatConfigPtr config);

    char EscapingTable[256];
    char UnescapingTable[256];

    const char* FindNextKeyStop(const char* begin, const char* end);
    const char* FindNextValueStop(const char* begin, const char* end);

#ifdef _YT_USE_SSE42_FOR_DSV_
#ifdef _MSC_VER
#define DECL_PREFIX __declspec(align(16))
#define DECL_SUFFIX
#else
#define DECL_PREFIX
#define DECL_SUFFIX __attribute__((aligned(16)))
#endif
    DECL_PREFIX __m128i KeyStopSymbols DECL_SUFFIX;
    DECL_PREFIX __m128i ValueStopSymbols DECL_SUFFIX;
    int NumberOfKeyStopSymbols;
    int NumberOfValueStopSymbols;
#else
    bool IsKeyStopSymbol[256];
    bool IsValueStopSymbol[256];
#endif


};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
