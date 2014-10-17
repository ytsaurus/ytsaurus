#pragma once

#include "public.h"

#include <vector>
#include <string>

#ifdef YT_USE_SSE42
#   include <nmmintrin.h>
#endif

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TLookupTable
{
public:
    TLookupTable();

    void Fill(const char* begin, const char* end);
    void Fill(const std::vector<char>& v);
    void Fill(const std::string& s);

    const char* FindNext(const char* begin, const char* end) const;

private:

#ifdef YT_USE_SSE42
#   ifdef _MSC_VER
#       define DECL_PREFIX __declspec(align(16))
#       define DECL_SUFFIX
#   else
#       define DECL_PREFIX
#       define DECL_SUFFIX __attribute__((aligned(16)))
#   endif
    DECL_PREFIX __m128i Symbols DECL_SUFFIX;
    int SymbolCount;
#endif
    bool Bitmap[256];

};

class TEscapeTable
{
public:
    TEscapeTable();

    char Forward[256];
    char Backward[256];
};

void WriteEscaped(
    TOutputStream* stream,
    const TStringBuf& string,
    const TLookupTable& lookupTable,
    const TEscapeTable& escapeTable,
    char escapingSymbol);

ui32 CalculateEscapedLength(
    const TStringBuf& string,
    const TLookupTable& lookupTable,
    const TEscapeTable& escapeTable,
    char escapingSymbol);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
