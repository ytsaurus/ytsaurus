#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include trace_context.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

inline TPosting MakePosting(ui32 blockIndex, TLineFingerprint lineFingerprint)
{
    return (blockIndex << BitsPerLineFingerprint) | lineFingerprint;
}

inline ui32 BlockIndexFromPosting(TPosting posting)
{
    return posting >> BitsPerLineFingerprint;
}

inline TLineFingerprint LineFingerprintFromPosting(TPosting posting)
{
    return posting & LineFingerprintMask;
}

inline TTrigram ReadTrigram(const char* src)
{
    return TTrigram(ReadUnaligned<ui32>(src) & 0xffffff);
}

inline TTrigram PackTrigram(char ch1, char ch2, char ch3)
{
    return TTrigram(
        (static_cast<TTrigram::TUnderlying>(ch3) << 16) |
        (static_cast<TTrigram::TUnderlying>(ch2) << 8) |
        static_cast<TTrigram::TUnderlying>(ch1));
}

inline std::array<char, 3> UnpackTrigram(TTrigram trigram)
{
    return {
        static_cast<char>(trigram.Underlying() & 0xff),
        static_cast<char>((trigram.Underlying() >> 8) & 0xff),
        static_cast<char>(trigram.Underlying() >> 16),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep
