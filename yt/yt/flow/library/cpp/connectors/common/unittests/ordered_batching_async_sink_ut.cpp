#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_batching_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>

#include <yt/yt/flow/library/cpp/common/distributing_tracker.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>

#include <util/generic/xrange.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("Test");

////////////////////////////////////////////////////////////////////////////////

class TTestSinkController
    : public TSinkControllerBase
{
public:
    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override
    {
        return 1;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestSink
    : public TOrderedBatchingAsyncSinkBase
{
public:
    using TSinkController = TTestSinkController;

    TTestSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext,
        std::shared_ptr<std::vector<std::pair<i64, std::vector<i64>>>> storage = {})
        : TOrderedBatchingAsyncSinkBase(std::move(context), std::move(dynamicContext))
        , Storage_(std::move(storage))
    { }

    void DoInit(const std::string& /*producerId*/) override
    { }

    TFuture<void> DoDistribute(const std::vector<TOutputMessageConstPtr>& messages, i64 seqNo) override
    {
        if (messages.empty()) {
            return OKFuture;
        }
        YT_TLOG_DEBUG("Received message batch")
            .With("SeqNo", seqNo)
            .With("MinMessageId", messages.front()->MessageId)
            .With("MaxMessageId", messages.back()->MessageId);
        if (!Storage_->empty() && Storage_->back().first >= seqNo) {
            YT_TLOG_DEBUG("SeqNo already commited")
                .With("SeqNo", seqNo);
            return OKFuture;
        }
        std::vector<i64> data;
        for (const auto& message : messages) {
            data.push_back(GetColumnValue<i64>(*message, "data"));
        }
        Storage_->push_back(std::pair(seqNo, data));
        return OKFuture;
    }

private:
    std::shared_ptr<std::vector<std::pair<i64, std::vector<i64>>>> Storage_;
};

YT_FLOW_DEFINE_SINK(TTestSink);

TEST(TOrderedBatchingAsyncSinkTest, Recovery)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("data", EValueType::Int64),
    });

    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
    specs[TStreamId("test")][TStreamSpecId(1)] = streamSpec;
    auto specStorage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(specs),
        /*groupBySchema*/ New<TTableSchema>(),
        /*evaluatorCache*/ nullptr);

    auto makeTestMessage = [&] (i64 id) {
        TMessageBuilder builder("test", schema);
        builder.SetMessageId(TMessageId(LexicographicallySerialize(id)));
        builder.SetSystemTimestamp(TSystemTimestamp(1700000000));
        builder.SetAlignmentTimestamp(TSystemTimestamp(1700000000));
        builder.SetEventTimestamp(TSystemTimestamp(1700000000));
        builder.Payload().Set<i64>(id, "data");
        return New<TOutputMessage>(builder.Finish(), specStorage);
    };

    const i64 batchSize = 7;
    auto getBatchCount = [] (auto messagesCount) {
        return (messagesCount + batchSize - 1) / batchSize;
    };

    const struct
    {
        i64 BeforeFailPersisted = 10;
        i64 BeforeFailNotPersisted = 5;
        i64 AfterFail = 10;

        i64 BeforeFail = BeforeFailPersisted + BeforeFailNotPersisted;
        i64 Total = BeforeFail + AfterFail;
    } messagesCount;

    auto context = New<TSinkContext>();
    context->Logger = Logger;
    auto spec = New<TSinkSpec>();
    auto dynamicSpec = New<TDynamicSinkSpec>();
    dynamicSpec->Parameters->AddChild("max_rows_per_batch", NYTree::ConvertToNode(batchSize));
    spec->InputStreamIds = {"test"};
    spec->SinkClassName = TypeName<TTestSink>();
    context->SinkSpec = spec;

    auto dynamicSinkContext = New<TDynamicSinkContext>();
    dynamicSinkContext->DynamicSinkSpec = dynamicSpec;

    TStateManagerMockPtr stateManager = New<TStateManagerMock>();
    auto doSync = [&] (auto sink) {
        sink->Sync(nullptr);
        stateManager->Sync();
    };
    auto queue = std::make_shared<std::vector<std::pair<i64, std::vector<i64>>>>();
    int expectedQueueSize = 0;

    std::vector<i64> expectedIds;
    std::vector<TOutputMessageConstPtr> messages;
    for (i64 id = 0; id < messagesCount.Total; id++) {
        messages.push_back(makeTestMessage(id));
        expectedIds.push_back(id);
    }

    // Helper: distribute a message and return a flag that flips when the callback fires.
    auto distributeWithFlag = [&] (auto sink, const TOutputMessageConstPtr& message) {
        auto fired = std::make_shared<std::atomic<bool>>(false);
        auto tracker = TDistributingTracker([fired] {
            fired->store(true);
        });
        sink->Distribute(message, tracker.AddDestination());
        tracker.Activate();
        return fired;
    };

    // Failed worker.
    {
        auto failedSink = New<TTestSink>(context, dynamicSinkContext, queue);
        failedSink->Init(stateManager->CreateContext());

        std::vector<std::shared_ptr<std::atomic<bool>>> firedFlags;

        // Persisted epoch.
        for (int i : xrange(messagesCount.BeforeFailPersisted)) {
            firedFlags.push_back(distributeWithFlag(failedSink, messages.at(i)));
        }
        doSync(failedSink);
        EXPECT_EQ(std::ssize(*queue), expectedQueueSize);
        failedSink->Commit();
        expectedQueueSize += getBatchCount(messagesCount.BeforeFailPersisted);
        EXPECT_EQ(std::ssize(*queue), expectedQueueSize);
        for (const auto& fired : firedFlags) {
            EXPECT_FALSE(fired->load());
        }

        // Empty epochs.
        doSync(failedSink);
        failedSink->Commit();
        for (const auto& fired : firedFlags) {
            EXPECT_TRUE(fired->load());
        }
        doSync(failedSink);
        failedSink->Commit();
        doSync(failedSink);
        failedSink->Commit();

        // Not persisted epoch.
        for (int i : xrange(messagesCount.BeforeFailPersisted, messagesCount.BeforeFail)) {
            firedFlags.push_back(distributeWithFlag(failedSink, messages.at(i)));
        }
        doSync(failedSink);
        EXPECT_EQ(std::ssize(*queue), expectedQueueSize);
        expectedQueueSize += getBatchCount(messagesCount.BeforeFailNotPersisted);
        failedSink->Commit();
        EXPECT_EQ(std::ssize(*queue), expectedQueueSize);
        for (int i : xrange(messagesCount.BeforeFailPersisted, messagesCount.BeforeFail)) {
            EXPECT_FALSE(firedFlags.at(i)->load());
        }
    }

    // New worker.
    {
        auto sink = New<TTestSink>(context, dynamicSinkContext, queue);
        sink->Init(stateManager->CreateContext());
        std::vector<std::shared_ptr<std::atomic<bool>>> firedFlags;
        for (auto i : xrange(messagesCount.BeforeFailPersisted, messagesCount.Total)) {
            firedFlags.push_back(distributeWithFlag(sink, messages.at(i)));
        }
        doSync(sink);
        EXPECT_EQ(std::ssize(*queue), expectedQueueSize);
        expectedQueueSize += getBatchCount(messagesCount.AfterFail);
        sink->Commit();
        EXPECT_EQ(std::ssize(*queue), expectedQueueSize);
        for (const auto& fired : firedFlags) {
            EXPECT_FALSE(fired->load());
        }
        doSync(sink);
        sink->Commit();
        for (const auto& fired : firedFlags) {
            EXPECT_TRUE(fired->load());
        }
    }

    std::vector<i64> gotIds;
    for (const auto& [seqNo, ids] : *queue) {
        for (const auto& id : ids) {
            gotIds.push_back(id);
        }
    }

    EXPECT_EQ(expectedIds, gotIds);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
