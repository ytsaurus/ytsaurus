#include "dsv_symbols.h"
#include "config.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

namespace {

#ifdef _YT_USE_SSE42_FOR_DSV_

const char _m128i_shift_right[31] = {
     0,  1,  2,  3,  4,  5,  6,  7,
     8,  9, 10, 11, 12, 13, 14, 15,
    -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1
};

static inline __m128i AlignedPrefixLoad(const void* p, int* length)
{
    int offset = (size_t)p & 15; *length = 16 - offset;

    if (offset) {
        // Load and shift to the right.
        // (Kudos to glibc authors for fast implementation).
        return _mm_shuffle_epi8(
            _mm_load_si128((__m128i*)((char*)p - offset)),
            _mm_loadu_si128((__m128i*)(_m128i_shift_right + offset)));
    } else {
        // Just load.
        return _mm_load_si128((__m128i*)p);
    }
}

static inline const char* FindNextSymbol(
    const char* begin,
    const char* end,
    __m128i symbols,
    int count)
{
    const char* current = begin;
    int length = end - begin;
    int result, result2, tmp;

    __m128i value = AlignedPrefixLoad(current, &tmp);

    do {
        // XXX(sandello): This compiles down to a single "pcmpestri $0x20,%xmm0,%xmm1"
        // instruction, because |result| is written to %ecx and |result2|
        // is written to CFlag, but CFlag checking is cheaper.
        // See http://software.intel.com/sites/default/files/m/0/3/c/d/4/18187-d9156103.pdf
        result = _mm_cmpestri(
            symbols,
            count,
            value,
            tmp,
            _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_ANY |
            _SIDD_MASKED_POSITIVE_POLARITY | _SIDD_LEAST_SIGNIFICANT);
        result2 = _mm_cmpestrc(
            symbols,
            count,
            value,
            tmp,
            _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_ANY |
            _SIDD_MASKED_POSITIVE_POLARITY | _SIDD_LEAST_SIGNIFICANT);

        if (result2) {
            return current + result;
        } else {
            current += tmp;
            length -= tmp;
        }

        if (length > 0) {
            value = _mm_load_si128((__m128i*)current);
            tmp = Min(16, length);
        } else {
            break;
        }
    } while(true);

    YASSERT(current == end);
    return current;
}

#else

static inline const char* FindNextSymbol(
    const char* begin,
    const char* end,
    const bool* matchTable)
{
    // XXX(sandello): Manual loop unrolling saves about 8% CPU.
    const char* current = begin;
#define DO_1  if (isStopSymbol[static_cast<ui8>(*current)]) { return current; } ++current;
#define DO_4  DO_1 DO_1 DO_1 DO_1
#define DO_16 DO_4 DO_4 DO_4 DO_4
    while (current + 16 < end) { DO_16; }
    while (current + 4  < end) { DO_4;  }
    while (current      < end) { DO_1;  }
#undef DO_1
#undef DO_4
#undef DO_16
    YASSERT(current == end);
    return current;
}

#endif

} // namespace anonymous


////////////////////////////////////////////////////////////////////////////////

TDsvSymbolTable::TDsvSymbolTable(TDsvFormatConfigPtr config)
{
    YCHECK(config);

    for (int i = 0; i < 256; ++i) {
        EscapingTable[i] = i;
        UnescapingTable[i] = i;
#ifndef _YT_USE_SSE42_FOR_DSV_
        IsKeyStopSymbol[i] = false;
        IsValueStopSymbol[i] = false;
#endif
    }

#ifdef _YT_USE_SSE42_FOR_DSV_
    KeyStopSymbols = _mm_setr_epi8(
        config->RecordSeparator,
        config->FieldSeparator,
        config->KeyValueSeparator,
        config->EscapingSymbol,
        '\0',
        0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0);

    ValueStopSymbols = _mm_setr_epi8(
        config->RecordSeparator,
        config->FieldSeparator,
        config->EscapingSymbol,
        '\0',
        0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0);

    NumberOfKeyStopSymbols = 5;
    NumberOfValueStopSymbols = 4;
#else
    IsKeyStopSymbol[static_cast<unsigned char>(config->RecordSeparator)] = true;
    IsKeyStopSymbol[static_cast<unsigned char>(config->FieldSeparator)] = true;
    IsKeyStopSymbol[static_cast<unsigned char>(config->KeyValueSeparator)] = true;
    IsKeyStopSymbol[static_cast<unsigned char>(config->EscapingSymbol)] = true;
    IsKeyStopSymbol[static_cast<unsigned char>('\0')] = true;

    IsValueStopSymbol[static_cast<unsigned char>(config->RecordSeparator)] = true;
    IsValueStopSymbol[static_cast<unsigned char>(config->FieldSeparator)] = true;
    IsValueStopSymbol[static_cast<unsigned char>(config->EscapingSymbol)] = true;
    IsValueStopSymbol[static_cast<unsigned char>('\0')] = true;
#endif

    EscapingTable['\0'] = '0';
    EscapingTable['\n'] = 'n';
    EscapingTable['\t'] = 't';

    UnescapingTable['0'] = '\0';
    UnescapingTable['t'] = '\t';
    UnescapingTable['n'] = '\n';
}

const char* TDsvSymbolTable::FindNextKeyStop(const char* begin, const char* end)
{
#ifdef _YT_USE_SSE42_FOR_DSV_
    return FindNextSymbol(begin, end, KeyStopSymbols, NumberOfKeyStopSymbols);
#else
    return FindNextSymbol(begin, end, IsKeyStopSymbol);
#endif
}

const char* TDsvSymbolTable::FindNextValueStop(const char* begin, const char* end)
{
#ifdef _YT_USE_SSE42_FOR_DSV_
    return FindNextSymbol(begin, end, ValueStopSymbols, NumberOfValueStopSymbols);
#else
    return FindNextSymbol(begin, end, IsValueStopSymbol);
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
