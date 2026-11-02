#pragma once

#include "timer_store.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! The timer store keeping its timers in pages of the |timer_pages| table; only the head pages
//! of the partition's families are resident.
struct IPagedTimerStore
    : public ITimerStore
{
    //! Synchronous forms of |Init| and |Preload|. |PreloadSync| returns whether a page was read.
    virtual void InitSync() = 0;
    virtual bool PreloadSync() = 0;
    //! Offers complete resident prefixes for export, regardless of stream watermarks and delays.
    virtual void TriggerAll() = 0;
    //! Commits the pending changes of the store together with |modify| in one transaction under the job's lease.
    virtual void SyncAndCommit(const std::function<void(const NApi::IDynamicTableTransactionPtr&)>& modify) = 0;
    //! Removes the empty pages of the partition, the last canonical one included, at the next sync.
    virtual void DropEmptyPages() = 0;
};

DEFINE_REFCOUNTED_TYPE(IPagedTimerStore);

IPagedTimerStorePtr CreatePagedTimerStore(TTimerStoreContextPtr context, TDynamicTimerStoreContextPtr dynamicContext);

//! Returns the key range of the partition; a partition with a source key gets the range of that exact key.
TKeyRange GetPartitionKeyRange(const TPartition& partition);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
