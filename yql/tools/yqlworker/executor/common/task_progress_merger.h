#pragma once

#include <util/generic/ptr.h>


namespace NYql {
namespace NProto {
    class TTaskProgress;
} // namespace NProto

struct TOperationProgress;

//////////////////////////////////////////////////////////////////////////////
// TTaskProgressMerger
//////////////////////////////////////////////////////////////////////////////
class TTaskProgressMerger {
public:
    TTaskProgressMerger();
    ~TTaskProgressMerger();

    void MergeWith(const TOperationProgress& progress);
    void MergeWith(const NProto::TTaskProgress& progress);
    void FlushTo(NProto::TTaskProgress* progress, bool onlyDirty = true);
    bool HasChangesSinceLastFlush() const;
    void AbortAllUnfinishedNodes();

private:
    struct TImpl;
    THolder<TImpl> Impl_;
};

} // namespace NYql
