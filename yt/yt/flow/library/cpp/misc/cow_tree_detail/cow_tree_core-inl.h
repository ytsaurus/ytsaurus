#ifndef COW_TREE_CORE_INL_H
    #error "Direct inclusion of this file is not allowed, include cow_tree_core.h"
    // For the sake of sane code completion.
    #include "cow_tree_core.h"
#endif

#include <util/system/guard.h>

namespace NYT::NFlow::NCowTreeInternals {

////////////////////////////////////////////////////////////////////////////////

template <class TConfig>
void TCowTreeCore<TConfig>::RegisterSnapshot(TViewLink* view, TViewLink* source)
{
    TGuard<TMutex> guard(Mutex_);
    if (source->IsSnapshot) {
        view->LinkAfter(source);
    } else {
        Snapshots_.PushFront(view);
        SetMaxLiveSnapshotVersion(view->SnapshotVersion);
    }
}

template <class TConfig>
void TCowTreeCore<TConfig>::UnregisterSnapshot(TViewLink* view)
{
    TGuard<TMutex> guard(Mutex_);
    bool newest = Snapshots_.Front() == view;
    view->Unlink();
    if (newest) {
        SetMaxLiveSnapshotVersion(Snapshots_.Empty() ? 0 : Snapshots_.Front()->SnapshotVersion);
    }
}

template <class TConfig>
ui64 TCowTreeCore<TConfig>::GetMaxLiveSnapshotVersion() const
{
    if constexpr (SingleThreaded) {
        return MaxLiveSnapshotVersion_;
    } else {
        return MaxLiveSnapshotVersion_.load(std::memory_order_acquire);
    }
}

template <class TConfig>
void TCowTreeCore<TConfig>::SetMaxLiveSnapshotVersion(ui64 version)
{
    if constexpr (SingleThreaded) {
        MaxLiveSnapshotVersion_ = version;
    } else {
        MaxLiveSnapshotVersion_.store(version, std::memory_order_release);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCowTreeInternals
