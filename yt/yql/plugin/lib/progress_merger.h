#pragma once

#include <yql/essentials/core/progress_merger/progress_merger.h>

#include <yql/tools/yqlworker/interface/proto/task.pb.h>
#include <yql/tools/yqlworker/interface/progress/task_node_progress.h>

#include <library/cpp/yson/writer.h>

namespace NYT::NYqlPlugin {

//////////////////////////////////////////////////////////////////////////////

class TNodeProgress
    : public NYql::TNodeProgress
{
public:
    using NYql::TNodeProgress::TNodeProgress;

    void Serialize(::NYson::TYsonWriter& yson) const;
    bool HasStages() const;
};

//////////////////////////////////////////////////////////////////////////////

class TProgressMerger : public NYql::NProgressMerger::ITaskProgressMerger {
public:
    void MergeWith(const NYql::TOperationProgress& progress) override;
    void MergeWith(const NYql::NProto::TTaskProgress& taskProgress, uint32_t revision = 0);
    void AbortAllUnfinishedNodes() override;

    bool HasChangesSinceLastFlush() const;
    TString ToYsonString();

private:
    bool HasChanges_ = false;
    uint32_t LastRevision_ = 0;
    THashMap<ui32, TNodeProgress> NodesMap_;
};

//////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
