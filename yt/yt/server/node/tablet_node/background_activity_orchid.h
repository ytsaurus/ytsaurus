#pragma once

#include "public.h"
#include "private.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

// Provides orchid for background activities.
// Runtime data is consistent inside one task, but can be inconsistent with data about task
// status (Pending, Started...), for example it is possible that Pending task has reader/writer statistics.
template <class TTaskInfo>
class TBackgroundActivityOrchid final
{
    static constexpr auto& Logger = TabletNodeLogger;

    using TTaskInfoPtr = TIntrusivePtr<TTaskInfo>;
    using TTaskMap = THashMap<TGuid, TTaskInfoPtr>;

public:
    explicit TBackgroundActivityOrchid(const TStoreBackgroundActivityOrchidConfigPtr& config);

    void Reconfigure(const TStoreBackgroundActivityOrchidConfigPtr& config);

    void AddPendingTasks(std::vector<TTaskInfoPtr>&& tasks);

    // Do not call this methods if you use TGuardedTaskInfo for
    // orchid task lifetime tracking.
    // XXX(dave11ar) Move it to private and add friend TGuardedTaskInfo?
    void OnTaskAborted(TGuid taskId);
    void OnTaskStarted(TGuid taskId);
    void OnTaskFailed(TGuid taskId);
    void OnTaskCompleted(TGuid taskId);

    void Serialize(NYson::IYsonConsumer* consumer) const;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TTaskMap PendingTasks_;
    TTaskMap RunningTasks_;
    std::deque<TTaskInfoPtr> FailedTasks_;
    std::deque<TTaskInfoPtr> CompletedTasks_;
    int MaxFailedTaskCount_;
    int MaxCompletedTaskCount_;

    void OnTaskFinished(TGuid taskId, std::deque<TTaskInfoPtr>* deque, int maxTaskCount);

    void ShrinkDeque(std::deque<TTaskInfoPtr>* deque, int targetSize);
    std::vector<TTaskInfoPtr> GetFromHashMap(const TTaskMap& source) const;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EBackgroundActivityTaskFinishStatus,
    (Aborted)
    (Failed)
    (Completed)
);

struct TBackgroundActivityTaskInfoBase
    : public TRefCounted
{
    struct TBaseStatistics
    {
        i64 ChunkCount = 0;
        i64 UncompressedDataSize = 0;
        i64 CompressedDataSize = 0;

        TBaseStatistics() = default;

        TBaseStatistics(i64 chunkCount, i64 uncompressedDataSize, i64 compressedDataSize);
        explicit TBaseStatistics(const NChunkClient::NProto::TDataStatistics& dataStatistics);
    };

    struct TReaderStatistics
        : public TBaseStatistics
    {
        i64 UnmergedRowCount = 0;
        i64 UnmergedDataWeight = 0;

        TReaderStatistics() = default;

        template <class TStorePtr>
        explicit TReaderStatistics(const std::vector<TStorePtr>& stores);
        explicit TReaderStatistics(const NChunkClient::NProto::TDataStatistics& dataStatistics);
    };

    struct TWriterStatistics
        : public TBaseStatistics
    {
        i64 RowCount = 0;
        i64 DataWeight = 0;

        TWriterStatistics() = default;

        TWriterStatistics(i64 chunkCount, i64 uncompressedDataSize, i64 compressedDataSize, i64 rowCount, i64 dataWeight);
        explicit TWriterStatistics(const NChunkClient::NProto::TDataStatistics& dataStatistics);

        TWriterStatistics operator-(const TWriterStatistics& other) const;
        TWriterStatistics& operator+=(const TWriterStatistics& other);
    };

    struct TRuntimeData
    {
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock);

        TInstant StartTime;
        TInstant FinishTime;
        std::optional<TError> Error;
        bool ShowStatistics = false;
        TWriterStatistics ProcessedWriterStatistics;
    };

    const TGuid TaskId;
    const TTabletId TabletId;
    const NHydra::TRevision MountRevision;
    const TString TablePath;
    const std::string TabletCellBundle;

    TBackgroundActivityTaskInfoBase(
        TGuid taskId,
        TTabletId tabletId,
        NHydra::TRevision mountRevision,
        TString tablePath,
        std::string tabletCellBundle);
};

DEFINE_REFCOUNTED_TYPE(TBackgroundActivityTaskInfoBase);

void Serialize(const TBackgroundActivityTaskInfoBase::TReaderStatistics& statistics, NYson::IYsonConsumer* consumer);
void Serialize(const TBackgroundActivityTaskInfoBase::TWriterStatistics& statistics, NYson::IYsonConsumer* consumer);

void SerializeFragment(const TBackgroundActivityTaskInfoBase::TRuntimeData& runtimeData, NYson::IYsonConsumer* consumer);

void Serialize(const TBackgroundActivityTaskInfoBase& task, NYson::IYsonConsumer* consumer);

template <class TTaskInfo>
class TGuardedTaskInfo
{
public:
    const TIntrusivePtr<TTaskInfo> Info;

    TGuardedTaskInfo(
        TIntrusivePtr<TTaskInfo> info,
        TIntrusivePtr<TBackgroundActivityOrchid<TTaskInfo>> orchid);
    ~TGuardedTaskInfo();

    TTaskInfo* operator->();

    // If task becomes started, it will be considered as completed when it ends,
    // unless OnFailed was called.
    void OnStarted();

    void OnFailed(TError error);

    bool IsFailed() const;

private:
    // No race with setting FinishStatus without lock because task destructor
    // in flush and compaction will be called in automaton thread.
    // Consider making this field atomic if setting FinishStatus and TTaskInfo destructor will
    // be called in different threads.
    EBackgroundActivityTaskFinishStatus FinishStatus_ = EBackgroundActivityTaskFinishStatus::Aborted;

    TIntrusivePtr<TBackgroundActivityOrchid<TTaskInfo>> Orchid_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

#define BACKGROUND_ACTIVITY_ORCHID_INL_H_
#include "background_activity_orchid-inl.h"
#undef BACKGROUND_ACTIVITY_ORCHID_INL_H_
