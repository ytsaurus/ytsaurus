#pragma once

#include "private.h"

#include "chunk_pool.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EChunkMappingMode,
    (Sorted)
    (Unordered)
    (SortedWithoutKeyGuarantree)
);

//! This class is companion for IPersistentChunkPoolInput.
//! During the operation lifetime an input chunk may be suspended and replaced with
//! another chunks (or chunks) on resumption. We keep the mapping that
//! provides for each original input chunk all its substitutes.
//!
//! Whenever we extract a stripe list from the chunk pool output,
//! we use the chunk mapping to replace the original chunks with their
//! substitutes.
//!
//! If an inconsistency during replacement is detected and task decides to continue
//! working (invalidating all current jobs as a result), there is an option to
//! force reset the contradicting chunk stripe to the given state and remap
//! all the remaining stripes.
class TInputChunkMapping
    : public TRefCounted
{
public:
    //! Used for persistence only.
    TInputChunkMapping() = default;

    explicit TInputChunkMapping(EChunkMappingMode mode);

    //! Modify given stripe, replacing all the input chunks with their current
    //! substitutes.
    NChunkPools::TChunkStripePtr GetMappedStripe(const NChunkPools::TChunkStripePtr& stripe) const;

    //! Given the knowledge that the old stripe list transformed into the new stripe list,
    //! populate mapping with the new input chunk correspondences, or report an error
    //! in case of inconsistent transformation (for example, when new stripe contains
    //! more data slices, or the new data slices have different read limits or boundary keys).
    void OnStripeRegenerated(
        NChunkPools::IChunkPoolInput::TCookie cookie,
        const NChunkPools::TChunkStripePtr& newStripe);

    //! Special case of that is used when unavailable input chunk strategy is Skip,
    //! and chunk disappears.
    void OnChunkDisappeared(const NChunkClient::TInputChunkPtr& chunk);

    //! Is called after chunk pool invalidation to force new stripe for the given input
    //! cookie and remap all the remaining stripes.
    void Reset(NChunkPools::IChunkPoolInput::TCookie, const NChunkPools::TChunkStripePtr& newStripe);

    void Persist(const TPersistenceContext& context);

    // TODO(max42): rename this method and the corresponding method of IChunkPollInput.
    void Add(NChunkPools::IChunkPoolInput::TCookie cookie, const NChunkPools::TChunkStripePtr& stripe);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    EChunkMappingMode Mode_;
    THashMap<NChunkClient::TInputChunkPtr, TCompactVector<NChunkClient::TInputChunkPtr, 1>> Substitutes_;
    THashMap<NChunkPools::IChunkPoolInput::TCookie, NChunkPools::TChunkStripePtr> OriginalStripes_;

    NChunkPools::TChunkStripePtr GetMappedStripeGuarded(const NChunkPools::TChunkStripePtr& stripe) const;

    static void ValidateSortedChunkConsistency(
        const NChunkClient::TInputChunkPtr& oldChunk,
        const NChunkClient::TInputChunkPtr& newChunk);
};

DEFINE_REFCOUNTED_TYPE(TInputChunkMapping)

extern TInputChunkMappingPtr IdentityChunkMapping;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
