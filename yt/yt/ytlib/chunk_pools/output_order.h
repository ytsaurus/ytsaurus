#pragma once

#include "private.h"

#include <yt/yt/ytlib/chunk_client/public.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

class TOutputOrder
    : public TRefCounted
{
public:
    using TCookie = TOutputCookie;

    class TEntry
    {
    public:
        NChunkClient::TInputChunkPtr GetTeleportChunk() const;
        TCookie GetCookie() const;

        bool IsTeleportChunk() const;
        bool IsCookie() const;

        TEntry(NChunkClient::TInputChunkPtr teleportChunk);
        TEntry(TCookie cookie);
        //! Used only for persistence.
        TEntry();

        bool operator == (const TEntry& other) const;

    private:
        using TContent = std::variant<NChunkClient::TInputChunkPtr, int>;
        TContent Content_;

        PHOENIX_DECLARE_TYPE(TEntry, 0xf504d386);
    };

    TOutputOrder() = default;

    void SeekCookie(TCookie cookie);
    void Push(TEntry entry);

    int GetSize() const;

    std::vector<NChunkClient::TChunkTreeId> ArrangeOutputChunkTrees(
        std::vector<std::pair<TEntry, NChunkClient::TChunkTreeId>> chunkTrees);

    std::vector<TEntry> ToEntryVector() const;

private:
    std::vector<int> CookieToPosition_;
    THashMap<NChunkClient::TInputChunkPtr, int> TeleportChunkToPosition_;

    std::vector<TEntry> Pool_;
    std::vector<int> NextPosition_;

    int CurrentPosition_ = -1;

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_TYPE(TOutputOrder, 0x7b6023e);
};

DEFINE_REFCOUNTED_TYPE(TOutputOrder)

void FormatValue(TStringBuilderBase* builder, const TOutputOrder::TEntry& entry, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
