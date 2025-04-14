#include "session.h"
#include "private.h"

#include <yt/yt/server/node/query_agent/config.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/query_client/query_service_proxy.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NQueryAgent {

using namespace NChunkClient;
using namespace NCompression;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NThreading;

struct TDistributedSessionRowsetTag
{ };

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = QueryAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TDistributedSession
    : public IDistributedSession
{
public:
    TDistributedSession(
        TDistributedSessionId sessionId,
        TLease lease,
        ECodec codecId,
        TDuration retentionTime,
        std::optional<i64> memoryLimitPerNode,
        IMemoryChunkProviderPtr memoryChunkProvider)
        : SessionId_(sessionId)
        , Lease_(std::move(lease))
        , CodecId_(codecId)
        , RetentionTime_(retentionTime)
        , MemoryLimitPerNode_(std::move(memoryLimitPerNode))
        , MemoryChunkProvider_(std::move(memoryChunkProvider))
    { }

    void InsertOrThrow(
        TRowsetId rowsetId,
        ISchemafulUnversionedReaderPtr reader,
        TTableSchemaPtr schema) override
    {
        auto rowBuffer = New<TRowBuffer>(TDistributedSessionRowsetTag(), MemoryChunkProvider_);

        auto futureRowset = BIND(MakeRowset, std::move(reader), std::move(schema), std::move(rowBuffer))
            .AsyncVia(GetCurrentInvoker())
            .Run();

        {
            auto guard = Guard(SessionLock_);

            auto [_, inserted] = RowsetMap_.emplace(rowsetId, std::move(futureRowset));
            THROW_ERROR_EXCEPTION_UNLESS(inserted,
                "Rowset %v is already present in session %v",
                rowsetId,
                SessionId_);
        }
    }

    TFuture<TSessionRowset> GetOrThrow(TRowsetId rowsetId) const override
    {
        auto guard = Guard(SessionLock_);

        auto it = RowsetMap_.find(rowsetId);
        if (it == RowsetMap_.end()) {
            THROW_ERROR_EXCEPTION("Rowset %v not found in session %v",
                rowsetId,
                SessionId_);
        }
        return it->second;
    }

    void RenewLease() const override
    {
        if (Lease_) {
            TLeaseManager::RenewLease(Lease_);
        }
    }

    std::vector<std::string> GetPropagationAddresses() const override
    {
        auto guard = Guard(SessionLock_);

        return {PropagationAddressQueue_.begin(), PropagationAddressQueue_.end()};
    }

    void ErasePropagationAddresses(const std::vector<std::string>& addresses) override
    {
        auto guard = Guard(SessionLock_);

        for (const auto& address : addresses) {
            PropagationAddressQueue_.erase(address);
        }
    }

    ECodec GetCodecId() const override
    {
        return CodecId_;
    }

    TFuture<void> PushRowset(
        const std::string& nodeAddress,
        TRowsetId rowsetId,
        TTableSchemaPtr schema,
        const std::vector<TRange<TUnversionedRow>>& subranges,
        INodeChannelFactoryPtr channelFactory,
        i64 desiredUncompressedBlockSize) override
    {
        auto proxy = TQueryServiceProxy(channelFactory->CreateChannel(nodeAddress));

        {
            YT_LOG_DEBUG("Propagating distributed session (SessionId: %v, NodeAddress: %v)",
                SessionId_,
                nodeAddress);

            auto request = proxy.CreateDistributedSession();
            ToProto(request->mutable_session_id(), SessionId_);
            request->set_retention_time(ToProto(RetentionTime_));
            request->set_codec(ToProto(CodecId_));
            if (MemoryLimitPerNode_) {
                request->set_memory_limit_per_node(*MemoryLimitPerNode_);
            }

            WaitFor(request->Invoke())
                .ValueOrThrow();
        }

        PropagateToNode(nodeAddress);

        auto rowsetEncoder = CreateWireProtocolRowsetWriter(
            CodecId_,
            desiredUncompressedBlockSize,
            schema,
            /*schemaful*/ true,
            QueryAgentLogger());

        bool ready = true;
        int rowCount = 0;
        for (const auto& subrange : subranges) {
            rowCount += subrange.Size();
            if (!ready) {
                WaitFor(rowsetEncoder->GetReadyEvent())
                    .ThrowOnError();
            }
            ready = rowsetEncoder->Write(subrange);
        }

        {
            YT_LOG_DEBUG("Pushing rowset (SessionId: %v, RowsetId: %v, RowCount: %v)",
                SessionId_,
                rowsetId,
                rowCount);

            auto request = proxy.PushRowset();
            ToProto(request->mutable_session_id(), SessionId_);
            ToProto(request->mutable_rowset_id(), rowsetId);
            ToProto(request->mutable_schema(), schema);

            request->Attachments() = rowsetEncoder->GetCompressedBlocks();

            return request->Invoke().AsVoid();
        }
    }

    const IMemoryChunkProviderPtr& GetMemoryChunkProvider() const override
    {
        return MemoryChunkProvider_;
    }

private:
    const TDistributedSessionId SessionId_;
    const TLease Lease_;
    const ECodec CodecId_;
    const TDuration RetentionTime_;
    const std::optional<i64> MemoryLimitPerNode_;
    const IMemoryChunkProviderPtr MemoryChunkProvider_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, SessionLock_);
    THashSet<std::string> PropagationAddressQueue_;
    THashMap<TRowsetId, TFuture<TSessionRowset>> RowsetMap_;

    void PropagateToNode(const std::string& address)
    {
        auto guard = Guard(SessionLock_);

        PropagationAddressQueue_.insert(address);
    }


    static TSessionRowset MakeRowset(
        ISchemafulUnversionedReaderPtr reader,
        TTableSchemaPtr schema,
        TRowBufferPtr rowBuffer)
    {
        std::vector<TUnversionedRow> rowset;
        i64 dataWeight = 0;

        while (auto batch = ReadRowBatch(reader)) {
            for (auto row : batch->MaterializeRows()) {
                // Could verify sortedness declared in schema, while we're at it.
                auto capturedRow = rowBuffer->CaptureRow(row);
                // TWireProtocolRowsetReader does not support GetDataStatistics method.
                dataWeight += GetDataWeight(capturedRow);
                rowset.push_back(capturedRow);
            }
        }

        return {
            MakeSharedRange(std::move(rowset), std::move(rowBuffer)),
            dataWeight,
            std::move(schema),
        };
    }
};

DEFINE_REFCOUNTED_TYPE(TDistributedSession)

////////////////////////////////////////////////////////////////////////////////

IDistributedSessionPtr CreateDistributedSession(
    TDistributedSessionId sessionId,
    TLease lease,
    ECodec codecId,
    TDuration retentionTime,
    std::optional<i64> memoryLimitPerNode,
    IMemoryChunkProviderPtr memoryChunkProvider)
{
    return New<TDistributedSession>(
        sessionId,
        std::move(lease),
        codecId,
        retentionTime,
        std::move(memoryLimitPerNode),
        std::move(memoryChunkProvider));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
