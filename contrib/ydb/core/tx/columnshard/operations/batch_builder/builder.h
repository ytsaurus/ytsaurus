#pragma once
#include <contrib/ydb/core/tx/columnshard/columnshard_private_events.h>
#include <contrib/ydb/core/tx/columnshard/common/snapshot.h>
#include <contrib/ydb/core/tx/columnshard/counters/columnshard.h>
#include <contrib/ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <contrib/ydb/core/tx/columnshard/operations/common/context.h>
#include <contrib/ydb/core/tx/conveyor/usage/abstract.h>
#include <contrib/ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NOlap {

class TBuildBatchesTask: public NConveyor::ITask, public NColumnShard::TMonitoringObjectsCounter<TBuildBatchesTask> {
private:
    NEvWrite::TWriteData WriteData;
    const TSnapshot ActualSnapshot;
    const TWritingContext Context;
    void ReplyError(const TString& message, const NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass errorClass);

protected:
    virtual void DoExecute(const std::shared_ptr<ITask>& taskPtr) override;

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "Write::ConstructBatches";
    }

    TBuildBatchesTask(NEvWrite::TWriteData&& writeData, const TWritingContext& context)
        : WriteData(std::move(writeData))
        , ActualSnapshot(context.GetApplyToSnapshot())
        , Context(context) {
        WriteData.MutableWriteMeta().OnStage(NEvWrite::EWriteStage::BuildBatch);
    }
};
}   // namespace NKikimr::NOlap
