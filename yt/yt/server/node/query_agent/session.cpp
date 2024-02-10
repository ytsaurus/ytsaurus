#include "session.h"
#include "private.h"

#include <yt/yt/server/node/query_agent/config.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/query_client/query_service_proxy.h>

#include <yt/yt/client/table_client/row_batch.h>
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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TDistributedSession
    : public IDistributedSession
{
public:
    TDistributedSession(
        TDistributedSessionId sessionId,
        TLease lease,
        ECodec codecId,
        TDuration retentionTime)
        : SessionId_(sessionId)
        , Lease_(std::move(lease))
        , CodecId_(codecId)
        , RetentionTime_(retentionTime)
    { }

    void InsertOrThrow(ISchemafulUnversionedReaderPtr reader, TRowsetId rowsetId) override
    {
        auto guard = Guard(SessionLock_);

        auto [_, inserted] = RowsetMap_.emplace(rowsetId, std::move(reader));
        THROW_ERROR_EXCEPTION_UNLESS(inserted,
            "Rowset %v is already present in session %v",
            rowsetId,
            SessionId_);
    }

    ISchemafulUnversionedReaderPtr GetOrThrow(TRowsetId rowsetId) const override
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

    std::vector<TString> GetPropagationAddresses() const override
    {
        auto guard = Guard(SessionLock_);

        return std::vector(PropagationAddressQueue_.begin(), PropagationAddressQueue_.end());
    }

    void ErasePropagationAddresses(const std::vector<TString>& addresses) override
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
        const TString& nodeAddress,
        TRowsetId rowsetId,
        TTableSchemaPtr schema,
        const std::vector<TRange<TUnversionedRow>>& subranges,
        INodeChannelFactoryPtr channelFactory,
        size_t desiredUncompressedBlockSize) override
    {
        auto proxy = TQueryServiceProxy(channelFactory->CreateChannel(nodeAddress));

        {
            YT_LOG_DEBUG("Propagating distributed session (SessionId: %v, NodeAddress: %v)",
                SessionId_,
                nodeAddress);

            auto request = proxy.CreateDistributedSession();
            ToProto(request->mutable_session_id(), SessionId_);
            request->set_retention_time(ToProto<i64>(RetentionTime_));
            request->set_codec(static_cast<int>(CodecId_));

            WaitFor(request->Invoke())
                .ValueOrThrow();
        }

        PropagateToNode(nodeAddress);

        auto rowsetEncoder = CreateWireProtocolRowsetWriter(
            CodecId_,
            desiredUncompressedBlockSize,
            schema,
            false,
            QueryAgentLogger);

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

private:
    const TDistributedSessionId SessionId_;
    const TLease Lease_;
    const ECodec CodecId_;
    const TDuration RetentionTime_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, SessionLock_);
    THashSet<TString> PropagationAddressQueue_;
    THashMap<TRowsetId, ISchemafulUnversionedReaderPtr> RowsetMap_;

    void PropagateToNode(TString address)
    {
        auto guard = Guard(SessionLock_);

        PropagationAddressQueue_.insert(std::move(address));
    }
};

DEFINE_REFCOUNTED_TYPE(TDistributedSession)

////////////////////////////////////////////////////////////////////////////////

IDistributedSessionPtr CreateDistributedSession(
    TDistributedSessionId sessionId,
    TLease lease,
    ECodec codecId,
    TDuration retentionTime)
{
    return New<TDistributedSession>(sessionId, std::move(lease), codecId, retentionTime);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
