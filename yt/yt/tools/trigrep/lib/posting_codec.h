#pragma once

#include "private.h"
#include "public.h"

#include <library/cpp/yt/memory/range.h>
#include <library/cpp/yt/memory/ref.h>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

//! Encodes/decodes the posting lists of one chunk index segment in a specific
//! on-disk format. A segment holds the concatenated encodings of a run of
//! trigrams' posting lists, each list a sorted, distinct sequence of postings.
struct IPostingCodec
{
    virtual ~IPostingCodec() = default;

    //! Upper bound on the bytes #Encode writes for a single list of at most
    //! #maxPostingCount postings over the universe |[0, postingUpperBound]|.
    virtual size_t GetMaxByteSize(int maxPostingCount, ui32 postingUpperBound) const = 0;

    //! Encodes one posting #list into #output, which must have room per
    //! #GetMaxByteSize; returns the one-past-end pointer.
    virtual char* Encode(char* output, TRange<TPosting> list, ui32 postingUpperBound) const = 0;

    //! Decodes #trigramCount posting lists (totalling #postingCount postings)
    //! from #data into #postings, setting #listStarts[0..trigramCount] to the
    //! per-list boundaries.
    virtual void Decode(
        TRef data,
        int trigramCount,
        int postingCount,
        ui32 postingUpperBound,
        TPosting* postings,
        TPosting** listStarts) const = 0;
};

//! Returns the stateless codec for #format.
IPostingCodec* GetPostingCodec(EIndexFormat format);

//! Maps a format to its file signature and back (see TIndexFileHeader).
ui64 GetIndexFormatSignature(EIndexFormat format);
EIndexFormat GetIndexFormatFromSignature(ui64 signature);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep
