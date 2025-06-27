#include "write_log.h"

#include "hunks_serialization.h"
#include "private.h"
#include "serialize.h"

#include <yt/yt/client/table_client/row_buffer.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TTransactionWriteRecord::TTransactionWriteRecord(
    TTabletId tabletId,
    TWireWriteCommandsBatch writeCommands,
    int rowCount,
    i64 dataWeight,
    const TSyncReplicaIdList& syncReplicaIds,
    const std::optional<NTableClient::THunkChunksInfo>& hunkChunksInfo)
    : TabletId(tabletId)
    , WriteCommands(std::move(writeCommands))
    , RowCount(rowCount)
    , DataWeight(dataWeight)
    , SyncReplicaIds(syncReplicaIds)
    , HunkChunksInfo(hunkChunksInfo)
{ }

void TTransactionWriteRecord::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, TabletId);
    Save(context, WriteCommands.Data_);
    Save(context, RowCount);
    Save(context, DataWeight);
    Save(context, SyncReplicaIds);
    Save(context, HunkChunksInfo);
}

void TTransactionWriteRecord::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, TabletId);

    Load(context, WriteCommands.Data_);
    WriteCommands.RowBuffer_ = New<NTableClient::TRowBuffer>();
    auto reader = CreateWireProtocolReader(WriteCommands.Data_, WriteCommands.RowBuffer_);
    WriteCommands.Commands_ = ParseWriteCommands(
        context.CurrentTabletWriteManagerSchemaData,
        reader.get(),
        context.CurrentTabletVersionedWriteIsUnversioned);

    Load(context, RowCount);
    Load(context, DataWeight);
    Load(context, SyncReplicaIds);
    Load(context, HunkChunksInfo);
}

i64 TTransactionWriteRecord::GetByteSize() const
{
    return WriteCommands.Data_.Size() + WriteCommands.Commands().capacity() * sizeof(TWireWriteCommands);
}

////////////////////////////////////////////////////////////////////////////////

i64 GetWriteLogRowCount(const TTransactionWriteLog& writeLog)
{
    i64 result = 0;
    for (const auto& entry : writeLog) {
        result += entry.RowCount;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TEnumeratingWriteLogReader::TEnumeratingWriteLogReader(const TTransactionIndexedWriteLog& writeLog)
    : WriteLogEnd_(writeLog.End())
    , WriteLogIterator_(writeLog.Begin())
    , WriteLogBatch_(WriteLogIterator_->WriteCommands.Commands())
{ }

TEnumeratingWriteLogReader::TEnumeratedWireWriteCommand TEnumeratingWriteLogReader::NextCommand()
{
    while (WriteLogBatch_.empty()) {
        ++WriteLogIterator_;
        ++CommandBatchIndex_;
        CommandIndexInBatch_ = 0;
        YT_VERIFY(WriteLogIterator_ != WriteLogEnd_);
        WriteLogBatch_ = WriteLogIterator_->WriteCommands.Commands();
    }

    const auto& command = WriteLogBatch_.front();
    WriteLogBatch_ = WriteLogBatch_.subspan(1);
    ++CommandIndexInBatch_;

    return {
        .Command = command,
        .WriteLogIndex = TOpaqueWriteLogIndex{
            .CommandBatchIndex = CommandBatchIndex_,
            .CommandIndexInBatch = CommandIndexInBatch_ - 1,
        },
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
