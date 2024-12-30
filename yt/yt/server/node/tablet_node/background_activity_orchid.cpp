#include "background_activity_orchid.h"

#include "store.h"

namespace NYT::NTabletNode {

using namespace NYson;
using namespace NYTree;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TBackgroundActivityTaskInfoBase::TBaseStatistics::TBaseStatistics(i64 ChunkCount, i64 UncompressedDataSize, i64 CompressedDataSize)
    : ChunkCount(ChunkCount)
    , UncompressedDataSize(UncompressedDataSize)
    , CompressedDataSize(CompressedDataSize)
{ }

TBackgroundActivityTaskInfoBase::TBaseStatistics::TBaseStatistics(const TDataStatistics& dataStatistics)
    : ChunkCount(dataStatistics.chunk_count())
    , UncompressedDataSize(dataStatistics.uncompressed_data_size())
    , CompressedDataSize(dataStatistics.compressed_data_size())
{ }

TBackgroundActivityTaskInfoBase::TReaderStatistics::TReaderStatistics(const TDataStatistics& dataStatistics)
    : TBaseStatistics(dataStatistics)
    , UnmergedRowCount(dataStatistics.unmerged_row_count())
    , UnmergedDataWeight(dataStatistics.unmerged_data_weight())
{ }

TBackgroundActivityTaskInfoBase::TWriterStatistics::TWriterStatistics(
    i64 chunkCount,
    i64 uncompressedDataSize,
    i64 compressedDataSize,
    i64 rowCount,
    i64 dataWeight)
    : TBaseStatistics(chunkCount, uncompressedDataSize, compressedDataSize)
    , RowCount(rowCount)
    , DataWeight(dataWeight)
{ }

TBackgroundActivityTaskInfoBase::TWriterStatistics::TWriterStatistics(const TDataStatistics& dataStatistics)
    : TBaseStatistics(dataStatistics)
    , RowCount(dataStatistics.row_count())
    , DataWeight(dataStatistics.data_weight())
{ }

TBackgroundActivityTaskInfoBase::TWriterStatistics TBackgroundActivityTaskInfoBase::TWriterStatistics::operator-(
    const TBackgroundActivityTaskInfoBase::TWriterStatistics& other) const
{
    return {
        ChunkCount - other.ChunkCount,
        UncompressedDataSize - other.UncompressedDataSize,
        CompressedDataSize - other.CompressedDataSize,
        RowCount - other.RowCount,
        DataWeight - other.DataWeight,
    };
}

TBackgroundActivityTaskInfoBase::TWriterStatistics& TBackgroundActivityTaskInfoBase::TWriterStatistics::operator+=(
    const TBackgroundActivityTaskInfoBase::TWriterStatistics& other)
{
    ChunkCount += other.ChunkCount;
    UncompressedDataSize += other.UncompressedDataSize;
    CompressedDataSize += other.CompressedDataSize;
    RowCount += other.RowCount;
    DataWeight += other.DataWeight;

    return *this;
}

TBackgroundActivityTaskInfoBase::TBackgroundActivityTaskInfoBase(
    TGuid taskId,
    TTabletId tabletId,
    NHydra::TRevision mountRevision,
    TString tablePath,
    TString tabletCellBundle)
    : TaskId(taskId)
    , TabletId(tabletId)
    , MountRevision(mountRevision)
    , TablePath(std::move(tablePath))
    , TabletCellBundle(std::move(tabletCellBundle))
{ }

void SerializeFragment(const TBackgroundActivityTaskInfoBase::TBaseStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonMapFragmentFluently(consumer)
        .Item("chunk_count").Value(statistics.ChunkCount)
        .Item("uncompressed_data_size").Value(statistics.UncompressedDataSize)
        .Item("compressed_data_size").Value(statistics.CompressedDataSize);
}

void Serialize(const TBackgroundActivityTaskInfoBase::TReaderStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Do([&] (auto fluent) {
                SerializeFragment(
                    static_cast<const TBackgroundActivityTaskInfoBase::TBaseStatistics&>(statistics),
                    fluent.GetConsumer());
            })
            .Item("unmerged_row_count").Value(statistics.UnmergedRowCount)
            .Item("unmerged_data_weight").Value(statistics.UnmergedDataWeight)
        .EndMap();
}

void Serialize(const TBackgroundActivityTaskInfoBase::TWriterStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Do([&] (auto fluent) {
                SerializeFragment(
                    static_cast<const TBackgroundActivityTaskInfoBase::TBaseStatistics&>(statistics),
                    fluent.GetConsumer());
            })
            .Item("row_count").Value(statistics.RowCount)
            .Item("data_weight").Value(statistics.DataWeight)
        .EndMap();
}

void SerializeFragment(const TBackgroundActivityTaskInfoBase::TRuntimeData& runtimeData, NYson::IYsonConsumer* consumer)
{
    YT_ASSERT_SPINLOCK_AFFINITY(runtimeData.SpinLock);

    BuildYsonMapFragmentFluently(consumer)
        .DoIf(static_cast<bool>(runtimeData.StartTime), [&] (auto fluent) {
            fluent
                .Item("start_time").Value(runtimeData.StartTime)
                .Item("duration").Value((runtimeData.FinishTime ? runtimeData.FinishTime : Now()) - runtimeData.StartTime);
        })
        .DoIf(static_cast<bool>(runtimeData.FinishTime), [&] (auto fluent) {
            fluent.Item("finish_time").Value(runtimeData.FinishTime);
        })
        .DoIf(runtimeData.Error.has_value(), [&] (auto fluent) {
            fluent.Item("error").Value(*runtimeData.Error);
        })
        .DoIf(runtimeData.ShowStatistics, [&] (auto fluent) {
            fluent.Item("processed_writer_statistics").Value(runtimeData.ProcessedWriterStatistics);
        });
}

void Serialize(const TBackgroundActivityTaskInfoBase& task, IYsonConsumer* consumer)
{
    BuildYsonMapFragmentFluently(consumer)
        .Item("trace_id").Value(task.TaskId)
        .Item("task_id").Value(task.TaskId)
        .Item("tablet_id").Value(task.TabletId)
        .Item("mount_revision").Value(task.MountRevision)
        .Item("table_path").Value(task.TablePath)
        .Item("tablet_cell_bundle").Value(task.TabletCellBundle);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
