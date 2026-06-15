#pragma once

#include <yql/essentials/core/progress_merger/progress_merger.h>

#include <yql/tools/yqlworker/interface/proto/task.pb.h>

namespace NYql {

//////////////////////////////////////////////////////////////////////////////

class TNodeProgress : public NProgressMerger::TNodeProgressBase {
public:
    using TNodeProgressBase::MergeWith;

    explicit TNodeProgress(const TOperationProgress& p);
    explicit TNodeProgress(const NProto::TTaskProgress::TNodeProgress& p);

    bool MergeWith(const NProto::TTaskProgress::TNodeProgress& p);
    void FlushTo(NProto::TTaskProgress::TNodeProgress* proto);

    static NProto::TTaskProgress::ENodeState ConvertState(EState s);
    static EState ConvertState(NProto::TTaskProgress::ENodeState s);
    static NProto::TTaskProgress::ENodeBlockStatus ConvertBlockStatus(TOperationProgress::EOpBlockStatus s);
    static TOperationProgress::EOpBlockStatus ConvertBlockStatus(NProto::TTaskProgress::ENodeBlockStatus s);
    static TVector<TOperationProgress::TStage> ProtoToStages(const NProto::TTaskProgress::TNodeProgress& proto);
    static void StagesToProto(const TVector<TOperationProgress::TStage>& stages, NProto::TTaskProgress::TNodeProgress* proto);
    static TVector<TOperationProgress::TAlert> ProtoToAlerts(const NProto::TTaskProgress::TNodeProgress& proto);
    static void AlertsToProto(const TVector<TOperationProgress::TAlert>& alerts, NProto::TTaskProgress::TNodeProgress* proto);
};

//////////////////////////////////////////////////////////////////////////////

} // namespace NYql
