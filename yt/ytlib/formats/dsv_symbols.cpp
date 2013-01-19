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

// This method performs an "aligned" load of |p| into 128-bit register.
// If |p| is not aligned then the returned value will contain a (byte-)prefix
// of memory region pointed by |p| truncated at the first 16-byte boundary.
// The length of the result is stored into |length|.
//
// Note that real motivation for this method is to avoid accidental page faults
// with direct unaligend reads. I. e., if you have 4 bytes at the end of a page
// then unaligned read will read 16 - 4 = 12 bytes from the next page causing
// a page fault; if the next page is unmapped this will incur a segmentation
// fault and terminate the process.
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
        // In short, PCMPxSTRx instruction takes two 128-bit registers with
        // packed bytes and performs string comparsion on them with user-defined
        // strategy. As the result PCMPxSTRx produces a lot of stuff, i. e.
        // match bit-mask, LSB or MSB of that bit-mask, and a few flags.
        //
        // See http://software.intel.com/sites/default/files/m/0/3/c/d/4/18187-d9156103.pdf
        //
        // In our case we are doing the following:
        //   - _SIDD_UBYTE_OPS - matching unsigned bytes,
        //   - _SIDD_CMP_EQUAL_ANY - comparing any byte from %xmm0 with any byte of %xmm1,
        //   - _SIDD_MASKED_POSITIVE_POLARITY - are interested only in proper bytes with positive matches,
        //   - _SIDD_LEAST_SIGNIFICANT - are interested in the index of least significant match position.
        //
        // In human terms this means "find position of first occurrence of
        // any byte from %xmm0 in %xmm1".
        //
        // XXX(sandello): These intrinsics compile down to a single
        // "pcmpestri $0x20,%xmm0,%xmm1" instruction, because |result| is written
        // to %ecx and |result2| is (simultaneously) written to CFlag.
        // We are interested in CFlag because it is cheaper to check.
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
#define DO_1  if (matchTable[static_cast<ui8>(*current)]) { return current; } ++current;
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
