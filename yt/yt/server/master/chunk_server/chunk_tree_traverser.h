#pragma once

#include "private.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct IChunkVisitor
    : public virtual TRefCounted
{
    /*!
     *  \note Return |false| to terminate traversing.
     */
    virtual bool OnChunk(
        TChunk* chunk,
        TChunkList* parent,
        std::optional<i64> rowIndex,
        std::optional<int> tabletIndex,
        const NChunkClient::TReadLimit& startLimit,
        const NChunkClient::TReadLimit& endLimit,
        const TChunkViewModifier* modifier) = 0;

    virtual void OnFinish(const TError& error) = 0;

    /*!
     *  \note Return |false| to traverse underlying chunk
     *      or |true| to skip it.
     */
    virtual bool OnChunkView(TChunkView* chunkView) = 0;

    /*!
     *  \note Return |false| to terminate traversing.
     */
    virtual bool OnDynamicStore(
        TDynamicStore* dynamicStore,
        std::optional<int> tabletIndex,
        const NChunkClient::TReadLimit& startLimit,
        const NChunkClient::TReadLimit& endLimit) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkVisitor)

////////////////////////////////////////////////////////////////////////////////

struct IChunkTraverserContext
    : public virtual TRefCounted
{
    //! If |true| then #GetInvoker and #GetUnsealedChunkStatistics are not supported;
    //! traverser must run fully synchronously. In particular, not all of its filtering
    //! features are available (e.g. it cannot handle non-trivial bounds).
    virtual bool IsSynchronous() const = 0;

    //! Returns |nullptr| if traversing cannot be preempted.
    virtual IInvokerPtr GetInvoker() const = 0;

    //! Called by the traverser to notify the context about the amount of
    //! time spent during traversing.
    virtual void OnTimeSpent(TDuration time) = 0;

    //! See NYT::NJournalClient::TChunkQuorumInfo.
    struct TUnsealedChunkStatistics
    {
        std::optional<i64> FirstOverlayedRowIndex;
        i64 RowCount = 0;
    };

    //! Asynchronously computes the statistics of an unsealed chunk.
    virtual TFuture<TUnsealedChunkStatistics> GetUnsealedChunkStatistics(TChunk* chunk) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkTraverserContext)

////////////////////////////////////////////////////////////////////////////////

IChunkTraverserContextPtr CreateAsyncChunkTraverserContext(
    NCellMaster::TBootstrap* bootstrap,
    NCellMaster::EAutomatonThreadQueue threadQueue);

IChunkTraverserContextPtr GetSyncChunkTraverserContext();

////////////////////////////////////////////////////////////////////////////////

struct TTraverserTestingOptions
{
    std::optional<int> MaxChunksPerIteration;
    std::optional<TDuration> DelayBetweenIterations;
};

////////////////////////////////////////////////////////////////////////////////

//! Traverses the subtree at #root pruning it to |lowerLimit:upperLimit| range.
//! For unsealed chunks, may consult the context to figure out the quorum information.
//! This call never visits hunk chunklists.
void TraverseChunkTree(
    IChunkTraverserContextPtr context,
    IChunkVisitorPtr visitor,
    TChunkList* root,
    const NChunkClient::TReadLimit& lowerLimit,
    const NChunkClient::TReadLimit& upperLimit,
    NTableClient::TComparator comparator,
    TTraverserTestingOptions testingOptions = {});

void TraverseChunkTree(
    IChunkTraverserContextPtr context,
    IChunkVisitorPtr visitor,
    const TChunkLists& roots,
    const NChunkClient::TReadLimit& lowerLimit,
    const NChunkClient::TReadLimit& upperLimit,
    NTableClient::TComparator comparator,
    TTraverserTestingOptions testingOptions = {});

//! Legacy version of previous function. Works by transforming legacy lower and upper
//! limits into new read limits and invoking previous version.
void TraverseChunkTree(
    IChunkTraverserContextPtr context,
    IChunkVisitorPtr visitor,
    TChunkList* root,
    const NChunkClient::TLegacyReadLimit& legacyLowerLimit,
    const NChunkClient::TLegacyReadLimit& legacyUpperLimit,
    NTableClient::TComparator comparator,
    TTraverserTestingOptions testingOptions = {});

void TraverseChunkTree(
    IChunkTraverserContextPtr context,
    IChunkVisitorPtr visitor,
    const TChunkLists& roots,
    const NChunkClient::TLegacyReadLimit& legacyLowerLimit,
    const NChunkClient::TLegacyReadLimit& legacyUpperLimit,
    NTableClient::TComparator comparator,
    TTraverserTestingOptions testingOptions = {});

//! Traverses the subtree at #root. No bounds are being checked,
//! #visitor is notified of each child in the subtree (including hunk chunks, if any).
void TraverseChunkTree(
    IChunkTraverserContextPtr context,
    IChunkVisitorPtr visitor,
    TChunkList* root,
    TTraverserTestingOptions testingOptions = {});

void TraverseChunkTree(
    IChunkTraverserContextPtr context,
    IChunkVisitorPtr visitor,
    const TChunkLists& roots,
    TTraverserTestingOptions testingOptions = {});

//! Appends the chunks found in subtree at #root to #chunks.
void EnumerateChunksInChunkTree(
    TChunkList* root,
    std::vector<TChunk*>* chunks);

//! Returns the list of all chunks in subtree at #root.
std::vector<TChunk*> EnumerateChunksInChunkTree(
    TChunkList* root);

//! Appends the chunks (including hunks), chunk views, and dynamic stores
//! found in subtree at #root to #stores.
void EnumerateStoresInChunkTree(
    TChunkList* root,
    std::vector<TChunkTree*>* stores);

//! Similar to #EnumerateStoresInChunkTree but returns a new vector
//! instead of appending to the existing one.
std::vector<TChunkTree*> EnumerateStoresInChunkTree(TChunkList* root);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
