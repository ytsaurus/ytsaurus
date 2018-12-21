#pragma once

#include "private.h"

#include <yt/ytlib/chunk_client/public.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

class TOutputOrder
    : public TRefCounted
{
public:
    using TCookie = TIntCookie;

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

        void Persist(const TPersistenceContext& context);
    private:
        using TContentType = TVariant<NChunkClient::TInputChunkPtr, int>;
        TContentType Content_;
    };

    TOutputOrder() = default;

    void SeekCookie(TCookie cookie);
    void Push(TEntry entry);

    int GetSize() const;

    std::vector<NChunkClient::TChunkTreeId> ArrangeOutputChunkTrees(
        std::vector<std::pair<TOutputOrder::TEntry, NChunkClient::TChunkTreeId>> chunkTrees);

    std::vector<TOutputOrder::TEntry> ToEntryVector() const;

    void Persist(const TPersistenceContext& context);

private:
    std::vector<int> CookieToPosition_;
    THashMap<NChunkClient::TInputChunkPtr, int> TeleportChunkToPosition_;

    std::vector<TEntry> Pool_;
    std::vector<int> NextPosition_;

    int CurrentPosition_ = -1;
};

DEFINE_REFCOUNTED_TYPE(TOutputOrder);

TString ToString(const TOutputOrder::TEntry& entry);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
