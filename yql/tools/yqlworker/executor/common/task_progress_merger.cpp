#include "task_progress_merger.h"

#include <yql/essentials/core/progress_merger/progress_merger.h>
#ifdef STATUS_PENDING
#undef STATUS_PENDING
#endif
#include <yql/tools/yqlworker/interface/proto/task.pb.h>
#include <yql/tools/yqlworker/interface/progress/task_node_progress.h>


namespace NYql {

using namespace NProgressMerger;


//////////////////////////////////////////////////////////////////////////////
// TTaskProgressMerger::TImpl
//////////////////////////////////////////////////////////////////////////////
struct TTaskProgressMerger::TImpl : public ITaskProgressMerger {
    TAdaptiveLock NodesLock;
    TAtomic HasChanges = 0;
    THashMap<ui32, TNodeProgress> NodesMap;


    void MergeWith(const TOperationProgress& progress) {
        with_lock (NodesLock) {
            auto in = NodesMap.emplace(progress.Id, progress);
            bool hasChanges = in.second; // new node was inserted?
            if (!in.second) { // node already exists
                hasChanges |= in.first->second.MergeWith(progress);
            }
            if (hasChanges) {
                AtomicSet(HasChanges, 1);
            }
        }
    }

    void MergeWith(const NProto::TTaskProgress& taskProgress) {
        with_lock (NodesLock) {
            bool hasChanges = false;
            for (const auto& nodeProgress: taskProgress.GetNodes()) {
                auto in = NodesMap.emplace(nodeProgress.GetId(), nodeProgress);
                hasChanges |= in.second; // new node was inserted?
                if (!in.second) { // node already exists
                    hasChanges |= in.first->second.MergeWith(nodeProgress);
                }
            }
            if (hasChanges) {
                AtomicSet(HasChanges, 1);
            }
        }
    }

    void FlushTo(NProto::TTaskProgress* taskProgress, bool onlyDirty) {
        with_lock (NodesLock) {
            for (auto& node: NodesMap) {
                if (!onlyDirty || node.second.IsDirty()) {
                    auto* nodeProgress = taskProgress->AddNodes();
                    nodeProgress->SetId(node.first);
                    node.second.FlushTo(nodeProgress);
                }
           }
           AtomicSet(HasChanges, 0);
        }
    }

    bool HasChangesSinceLastFlush() const {
        return AtomicGet(HasChanges) != 0;
    }

    void AbortAllUnfinishedNodes() {
        with_lock (NodesLock) {
            bool hasChanges = false;
            for (auto& node: NodesMap) {
                if (node.second.IsUnfinished()) {
                    node.second.Abort();
                    hasChanges = true;
                }
            }
            if (hasChanges) {
                AtomicSet(HasChanges, 1);
            }
        }
    }
};

//////////////////////////////////////////////////////////////////////////////
// TTaskProgressMerger
//////////////////////////////////////////////////////////////////////////////
TTaskProgressMerger::TTaskProgressMerger()
    : Impl_(new TImpl){
}

TTaskProgressMerger::~TTaskProgressMerger() {
}

void TTaskProgressMerger::MergeWith(const TOperationProgress& progress) {
    Impl_->MergeWith(progress);
}

void TTaskProgressMerger::MergeWith(const NProto::TTaskProgress& progress) {
    Impl_->MergeWith(progress);
}

void TTaskProgressMerger::FlushTo(
        NProto::TTaskProgress* progress, bool onlyDirty)
{
    Impl_->FlushTo(progress, onlyDirty);
}

bool TTaskProgressMerger::HasChangesSinceLastFlush() const {
    return Impl_->HasChangesSinceLastFlush();
}

void TTaskProgressMerger::AbortAllUnfinishedNodes() {
    Impl_->AbortAllUnfinishedNodes();
}

} // namespace NYql
