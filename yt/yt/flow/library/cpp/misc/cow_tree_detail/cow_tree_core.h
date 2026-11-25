#pragma once

#include "cow_tree_value.h"

#include <util/generic/intrlist.h>

#include <util/system/types.h>

#include <atomic>
#include <type_traits>

namespace NYT::NFlow::NCowTreeInternals {

////////////////////////////////////////////////////////////////////////////////

//! A tree view's entry in the core's view list.
struct TViewLink
    : TIntrusiveListItem<TViewLink>
{
    ui64 SnapshotVersion = 0;
    bool IsSnapshot = false;
};

//! State shared by all views of one tree: the master and its snapshots. Kept alive
//! by view references, so a snapshot may freely outlive its master.
template <class TConfig>
class TCowTreeCore
{
public:
    using TMutex = typename TConfig::TMutex;

    //! With TNullMutex the whole view machinery is atomics-free: the tree and all its
    //! views are single-threaded by contract.
    static constexpr bool SingleThreaded = std::is_same_v<TMutex, TNullMutex>;

    typename TConfig::TRefCounter RefCounter;

    //! Bumped by the master on every real modification; written by the master only.
    ui64 Version = 1;

    void RegisterSnapshot(TViewLink* view, TViewLink* source);
    void UnregisterSnapshot(TViewLink* view);

    //! Acquiring a lower maximum synchronizes in-place writes with retired snapshot readers.
    ui64 GetMaxLiveSnapshotVersion() const;

private:
    [[no_unique_address]] TMutex Mutex_;
    //! Live snapshots in descending version order.
    TIntrusiveList<TViewLink> Snapshots_;
    std::conditional_t<SingleThreaded, ui64, std::atomic<ui64>> MaxLiveSnapshotVersion_{0};

    void SetMaxLiveSnapshotVersion(ui64 version);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCowTreeInternals

#define COW_TREE_CORE_INL_H
#include "cow_tree_core-inl.h"
#undef COW_TREE_CORE_INL_H
