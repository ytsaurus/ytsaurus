#include "progress_merger.h"

#include <yql/essentials/utils/log/log.h>

namespace NYT::NYqlPlugin {

//////////////////////////////////////////////////////////////////////////////

namespace {

constexpr int MaxProgressNodes = 25'000;

} // namespace

//////////////////////////////////////////////////////////////////////////////

void TNodeProgress::Serialize(::NYson::TYsonWriter& writer) const
{
    writer.OnBeginMap();
    {
        writer.OnKeyedItem("category");
        writer.OnStringScalar(Progress_.Category);

        writer.OnKeyedItem("state");
        writer.OnStringScalar(ToString(Progress_.State));

        writer.OnKeyedItem("remoteId");
        writer.OnStringScalar(Progress_.RemoteId);

        writer.OnKeyedItem("waitingRemoteId");
        writer.OnStringScalar(Progress_.WaitingRemoteId);

        writer.OnKeyedItem("remoteData");
        writer.OnBeginMap();
        for (const auto& it : Progress_.RemoteData) {
            writer.OnKeyedItem(it.first);
            writer.OnStringScalar(it.second);
        }
        writer.OnEndMap();

        writer.OnKeyedItem("stages");
        writer.OnBeginMap();
        for (size_t index = 0; index < Stages_.size(); index++) {
            writer.OnKeyedItem(ToString(index));
            writer.OnBeginMap();
            {
                writer.OnKeyedItem(Stages_[index].first);
                writer.OnStringScalar(Stages_[index].second.ToString());
            }
            writer.OnEndMap();
        }
        writer.OnEndMap();

        if (Progress_.Counters) {
            writer.OnKeyedItem("completed");
            writer.OnUint64Scalar(Progress_.Counters->Completed);

            writer.OnKeyedItem("running");
            writer.OnUint64Scalar(Progress_.Counters->Running);

            writer.OnKeyedItem("total");
            writer.OnUint64Scalar(Progress_.Counters->Total);

            writer.OnKeyedItem("aborted");
            writer.OnUint64Scalar(Progress_.Counters->Aborted);

            writer.OnKeyedItem("failed");
            writer.OnUint64Scalar(Progress_.Counters->Failed);

            writer.OnKeyedItem("lost");
            writer.OnUint64Scalar(Progress_.Counters->Lost);

            writer.OnKeyedItem("pending");
            writer.OnUint64Scalar(Progress_.Counters->Pending);
        }

        writer.OnKeyedItem("startedAt");
        writer.OnStringScalar(StartedAt_.ToString());

        if (FinishedAt_ != TInstant::Max()) {
            writer.OnKeyedItem("finishedAt");
            writer.OnStringScalar(FinishedAt_.ToString());
        }
    }
    writer.OnEndMap();
}

//////////////////////////////////////////////////////////////////////////////

void TProgressMerger::MergeWith(const NYql::TOperationProgress& progress)
{
    auto it = NodesMap_.find(progress.Id);
    if (it == NodesMap_.end()) {
        if (ssize(NodesMap_) >= MaxProgressNodes) {
            LogProgressNodeLimitExceededOnce(progress.Id);
            return;
        }
        NodesMap_.emplace(progress.Id, progress);
    } else {
        it->second.MergeWith(progress);
    }
    HasChanges_ = true;
}

void TProgressMerger::MergeWith(const NYql::NProto::TTaskProgress& taskProgress, std::optional<ui32> revision)
{
    for (const auto& node : taskProgress.GetNodes()) {
        auto it = NodesMap_.find(node.GetId());
        bool changed = false;
        if (it == NodesMap_.end()) {
            if (ssize(NodesMap_) >= MaxProgressNodes) {
                LogProgressNodeLimitExceededOnce(node.GetId());
                if (revision && !TruncationRevision_) {
                    TruncationRevision_ = *revision;
                }
                continue;
            }
            it = NodesMap_.emplace(node.GetId(), TNodeProgress(node)).first;
            changed = true;
        } else {
            changed = it->second.MergeWith(node);
        }
        HasChanges_ |= changed;
        if (changed && revision) {
            LatestProgressPartRevisions_[ToString(node.GetId())] = *revision;
        }
    }
}

void TProgressMerger::LogProgressNodeLimitExceededOnce(ui32 nodeId)
{
    if (!ProgressNodeLimitExceededLogged_) {
        YQL_LOG(WARN)
            << "Progress node limit reached; ignoring further new nodes and reporting yql_progress_truncated=true "
            << "(MaxProgressNodes: " << MaxProgressNodes
            << ", FirstIgnoredNodeId: " << nodeId << ")";
        ProgressNodeLimitExceededLogged_ = true;
        HasChanges_ = true;
    }
}

void TProgressMerger::AbortAllUnfinishedNodes()
{
    for (auto& node: NodesMap_) {
        if (node.second.IsUnfinished()) {
            node.second.Abort();
            HasChanges_ = true;
        }
    }
}

TString TProgressMerger::ToYsonString()
{
    TStringStream yson;
    ::NYson::TYsonWriter writer(&yson);

    writer.OnBeginMap();
    for (auto& node: NodesMap_) {
        writer.OnKeyedItem(ToString(node.first));
        node.second.Serialize(writer);
    }
    writer.OnEndMap();
    HasChanges_ = false;

    return yson.Str();
}

bool TProgressMerger::HasChangesSinceLastFlush() const
{
    return HasChanges_;
}

bool TProgressMerger::IsTruncated() const
{
    return ProgressNodeLimitExceededLogged_;
}

std::optional<ui32> TProgressMerger::GetTruncationRevision() const
{
    return TruncationRevision_;
}

THashMap<TString, std::pair<ui32, TString>> TProgressMerger::GetLatestProgressPartRevisions(
    std::optional<ui32> afterRevision) const
{
    THashMap<TString, std::pair<ui32, TString>> parts;
    for (const auto& [nodeId, revision] : LatestProgressPartRevisions_) {
        if (afterRevision && revision <= *afterRevision) {
            continue;
        }

        TStringStream yson;
        ::NYson::TYsonWriter writer(&yson);
        NodesMap_.at(FromString<ui32>(nodeId)).Serialize(writer);
        parts.emplace(nodeId, std::make_pair(revision, yson.Str()));
    }
    return parts;
}

//////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
