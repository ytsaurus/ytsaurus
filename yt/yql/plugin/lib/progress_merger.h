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
};

//////////////////////////////////////////////////////////////////////////////

class TProgressMerger : public NYql::NProgressMerger::ITaskProgressMerger {
public:
    void MergeWith(const NYql::TOperationProgress& progress) override;
    void MergeWith(const NYql::NProto::TTaskProgress& taskProgress, std::optional<ui32> revision);
    void AbortAllUnfinishedNodes() override;

    bool HasChangesSinceLastFlush() const;
    bool IsTruncated() const;
    std::optional<ui32> GetTruncationRevision() const;
    TString ToYsonString();
    THashMap<TString, std::pair<ui32, TString>> GetLatestProgressPartRevisions(
        std::optional<ui32> afterRevision) const;

private:
    bool HasChanges_ = false;
    bool ProgressNodeLimitExceededLogged_ = false;
    std::optional<ui32> TruncationRevision_;
    THashMap<ui32, TNodeProgress> NodesMap_;
    THashMap<TString, ui32> LatestProgressPartRevisions_;

    void LogProgressNodeLimitExceededOnce(ui32 nodeId);
};

//////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
