#include "public.h"

#include "serialize.h"
#include "write_commands.h"

#include <yt/yt/core/misc/persistent_queue.h>

#include <yt/yt/ytlib/table_client/hunks.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionWriteRecord
{
    TTransactionWriteRecord() = default;
    TTransactionWriteRecord(
        TTabletId tabletId,
        TWireWriteCommandsBatch writeCommands,
        int rowCount,
        i64 byteSize,
        const TSyncReplicaIdList& syncReplicaIds,
        const std::optional<NTableClient::THunkChunksInfo>& hunkChunksInfo);

    TTabletId TabletId;
    TWireWriteCommandsBatch WriteCommands;
    int RowCount = 0;
    i64 DataWeight = 0;
    TSyncReplicaIdList SyncReplicaIds;

    std::optional<NTableClient::THunkChunksInfo> HunkChunksInfo;

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    i64 GetByteSize() const;
};

////////////////////////////////////////////////////////////////////////////////

constexpr size_t TransactionWriteLogChunkSize = 256;
using TTransactionWriteLog = TPersistentQueue<TTransactionWriteRecord, TransactionWriteLogChunkSize>;
using TTransactionIndexedWriteLog = TIndexedPersistentQueue<TTransactionWriteRecord, TransactionWriteLogChunkSize>;
using TTransactionWriteLogSnapshot = TPersistentQueueSnapshot<TTransactionWriteRecord, TransactionWriteLogChunkSize>;

////////////////////////////////////////////////////////////////////////////////

i64 GetWriteLogRowCount(const TTransactionWriteLog& writeLog);

////////////////////////////////////////////////////////////////////////////////

class TEnumeratingWriteLogReader
{
public:
    struct TEnumeratedWireWriteCommand
    {
        const TWireWriteCommand& Command;
        TOpaqueWriteLogIndex WriteLogIndex;
    };

    explicit TEnumeratingWriteLogReader(const TTransactionIndexedWriteLog& writeLog);

    TEnumeratedWireWriteCommand NextCommand();

private:
    const TTransactionIndexedWriteLog::TIterator WriteLogEnd_;

    TTransactionIndexedWriteLog::TIterator WriteLogIterator_;
    std::span<const TWireWriteCommand> WriteLogBatch_;

    int CommandBatchIndex_ = 0;
    int CommandIndexInBatch_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
