#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/options.h>

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/rpc/channel_detail.h>
#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/message.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TPendingHunkWrite
{
    IClientResponseHandlerPtr Handler;
    TSharedRefArray Response;
};

class TDelayedHunkWriteChannel
    : public TChannelWrapper
{
public:
    TDelayedHunkWriteChannel(
        IChannelPtr underlyingChannel,
        TCallback<void(TPendingHunkWrite)> onWrite)
        : TChannelWrapper(std::move(underlyingChannel))
        , OnWrite_(std::move(onWrite))
    { }

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        if (request->GetService() != NTabletClient::TTabletServiceProxy::GetDescriptor().ServiceName ||
            request->GetMethod() != "WriteHunks")
        {
            return TChannelWrapper::Send(std::move(request), std::move(responseHandler), options);
        }

        auto hunkRequest = DynamicPointerCast<NTabletClient::TTabletServiceProxy::TReqWriteHunks>(request);
        NTabletClient::NProto::TRspWriteHunks response;
        for (const auto& payload : hunkRequest->Attachments()) {
            auto* descriptor = response.add_descriptors();
            ToProto(descriptor->mutable_chunk_id(), TGuid::Create());
            descriptor->set_record_index(0);
            descriptor->set_record_offset(0);
            descriptor->set_length(payload.Size() + sizeof(NTableClient::THunkPayloadHeader));
        }

        // The transport may release the request and its attachments before replying,
        // and cancellation need not prevent an already received response from arriving.
        OnWrite_(TPendingHunkWrite{
            .Handler = std::move(responseHandler),
            .Response = CreateResponseMessage(response),
        });
        // An unbound control thunk records cancellation without completing the RPC.
        return New<TClientRequestControlThunk>();
    }

private:
    const TCallback<void(TPendingHunkWrite)> OnWrite_;
};

////////////////////////////////////////////////////////////////////////////////

class THunkTransactionTest
    : public TDynamicTablesTestBase
{ };

TEST_F(THunkTransactionTest, LateWriteHunksResponseAfterCommitFailure)
{
    CreateTable(
        "//tmp/hunk_transaction",
        TYsonString("[{name=value;type=string;max_inline_hunk_size=1}]"_sb),
        /*mount*/ false);
    auto hunkStorageId = WaitFor(Client_->CreateNode("//tmp/hunk_storage", EObjectType::HunkStorage))
        .ValueOrThrow();
    WaitFor(Client_->SetNode(Table_ + "/@hunk_storage_id", ConvertToYsonString(hunkStorageId)))
        .ThrowOnError();
    SyncMountTable("//tmp/hunk_storage");
    SyncMountTable(Table_);

    auto queue = New<TActionQueue>("HunkTransactionTest");
    auto connection = NNative::CreateConnection(
        DynamicPointerCast<NNative::IConnection>(Connection_)->GetCompoundConfig(),
        NNative::TConnectionOptions(queue->GetInvoker()));
    auto terminateConnection = Finally([&] {
        connection->Terminate();
    });

    auto firstWritePromise = NewPromise<TPendingHunkWrite>();
    auto secondWritePromise = NewPromise<TPendingHunkWrite>();
    auto options = NNative::TClientOptions::Root();
    options.ChannelWrapper = BIND([=] (IChannelPtr channel) -> IChannelPtr {
        return New<TDelayedHunkWriteChannel>(std::move(channel), BIND([=] (TPendingHunkWrite write) {
            if (!firstWritePromise.TrySet(write)) {
                secondWritePromise.Set(std::move(write));
            }
        }));
    });
    auto client = connection->CreateNativeClient(options);
    auto transaction = WaitFor(client->StartTransaction(ETransactionType::Tablet))
        .ValueOrThrow();
    auto weakTransaction = MakeWeak(transaction);
    auto [rows, nameTable] = PrepareUnversionedRow({"value"}, "<id=0>payload");
    transaction->WriteRows(Table_, nameTable, rows);
    transaction->WriteRows(Table_, nameTable, rows);
    auto commitFuture = transaction->Commit();

    auto firstWrite = WaitFor(firstWritePromise.ToFuture().WithTimeout(TDuration::Seconds(30)))
        .ValueOrThrow();
    auto secondWrite = WaitFor(secondWritePromise.ToFuture().WithTimeout(TDuration::Seconds(30)))
        .ValueOrThrow();
    auto failPendingWrite = Finally([&] {
        if (secondWrite.Handler) {
            secondWrite.Handler->HandleError(TError("Test cleanup"));
        }
    });

    firstWrite.Handler->HandleError(TError("Injected hunk write failure"));
    auto commitError = WaitFor(commitFuture);
    EXPECT_FALSE(commitError.IsOK());
    EXPECT_THAT(ToString(commitError), ::testing::HasSubstr("Injected hunk write failure"));
    transaction.Reset();

    // Drain both invokers so completed RPC and commit callbacks no longer retain it.
    WaitFor(BIND([] { }).AsyncVia(TDispatcher::Get()->GetLightInvoker()).Run()).ThrowOnError();
    WaitFor(BIND([] { }).AsyncVia(queue->GetInvoker()).Run()).ThrowOnError();
    WaitUntil([&] { return weakTransaction.IsExpired(); }, "Transaction retained after the late response");

    secondWrite.Handler->HandleResponse(std::move(secondWrite.Response), /*address*/ "test");
    secondWrite.Handler.Reset();

    SyncUnmountTable(Table_);
    SyncUnmountTable("//tmp/hunk_storage");
    WaitFor(Client_->RemoveNode(Table_ + "/@hunk_storage_id"))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
