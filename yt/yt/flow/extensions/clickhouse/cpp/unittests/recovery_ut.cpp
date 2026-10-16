#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/clickhouse/cpp/shard.h>
#include <yt/yt/flow/extensions/clickhouse/cpp/sink.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_batching_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>

#include <yt/yt/flow/library/cpp/common/distributing_tracker.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

#include <util/generic/xrange.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("Test");

////////////////////////////////////////////////////////////////////////////////

class TRecoveryTestSinkController
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

struct TRecordedBatch
{
    std::string DedupToken;
    std::string ShardName;
    std::string ProducerId;
    std::vector<i64> Ids;
};

class TRecoveryTestSink
    : public TOrderedBatchingAsyncSinkBase
{
public:
    using TSinkController = TRecoveryTestSinkController;

    TRecoveryTestSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext,
        std::shared_ptr<std::vector<std::pair<i64, TRecordedBatch>>> storage = {},
        std::shared_ptr<TClickHouseShardRouter> router = {})
        : TOrderedBatchingAsyncSinkBase(std::move(context), std::move(dynamicContext))
        , Storage_(std::move(storage))
        , Router_(std::move(router))
    { }

    void DoInit(const std::string& producerId) override
    {
        ProducerId_ = producerId;
    }

    TFuture<void> DoDistribute(const std::vector<TOutputMessageConstPtr>& messages, i64 seqNo) override
    {
        if (messages.empty()) {
            return OKFuture;
        }
        if (!Storage_->empty() && Storage_->back().first >= seqNo) {
            return OKFuture;
        }

        auto batchDedupToken = BuildDedupToken(messages);
        const auto& shards = Router_->GetShards();
        std::vector<std::vector<i64>> idsByShard(shards.size());
        for (const auto& message : messages) {
            idsByShard[Router_->SelectShard(message)].push_back(GetColumnValue<i64>(*message, "data"));
        }
        for (int shardIndex = 0; shardIndex < std::ssize(shards); ++shardIndex) {
            if (idsByShard[shardIndex].empty()) {
                continue;
            }
            Storage_->push_back(std::pair(seqNo, TRecordedBatch{
                    .DedupToken = BuildShardDedupToken(batchDedupToken, shards[shardIndex]),
                    .ShardName = shards[shardIndex].Name,
                    .ProducerId = ProducerId_,
                    .Ids = std::move(idsByShard[shardIndex]),
                                                 }));
        }
        return OKFuture;
    }

private:
    const std::shared_ptr<std::vector<std::pair<i64, TRecordedBatch>>> Storage_;
    const std::shared_ptr<TClickHouseShardRouter> Router_;
    std::string ProducerId_;
};

YT_FLOW_DEFINE_SINK(TRecoveryTestSink);

////////////////////////////////////////////////////////////////////////////////

void CheckRecovery(TStringBuf parametersYson)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("data", EValueType::Int64),
    });

    auto sinkParameters = NYTree::ConvertTo<TCommonClickHouseSinkParametersPtr>(
        NYson::TYsonString(parametersYson));
    auto router = std::make_shared<TClickHouseShardRouter>(
        ResolveShards(*sinkParameters),
        std::vector<std::string>{"data"},
        std::vector<TTableSchemaPtr>{schema});

    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
    specs[TStreamId("test")][TStreamSpecId(1)] = streamSpec;
    auto specStorage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(specs),
        New<TTableSchema>(),
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
    auto getBatchCount = [&] (i64 count) {
        return (count + batchSize - 1) / batchSize;
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
    spec->SinkClassName = TypeName<TRecoveryTestSink>();
    context->SinkSpec = spec;

    auto dynamicSinkContext = New<TDynamicSinkContext>();
    dynamicSinkContext->DynamicSinkSpec = dynamicSpec;

    TStateManagerMockPtr stateManager = New<TStateManagerMock>();
    auto doSync = [&] (auto sink) {
        sink->Sync(nullptr);
        stateManager->Sync();
    };

    auto storage = std::make_shared<std::vector<std::pair<i64, TRecordedBatch>>>();
    int expectedBatchCount = 0;

    auto countDistinctSeqNos = [&] {
        THashSet<i64> seqNos;
        for (const auto& [seqNo, batch] : *storage) {
            seqNos.insert(seqNo);
        }
        return std::ssize(seqNos);
    };

    std::vector<i64> expectedIds;
    std::vector<TOutputMessageConstPtr> messages;
    for (i64 id = 0; id < messagesCount.Total; ++id) {
        messages.push_back(makeTestMessage(id));
        expectedIds.push_back(id);
    }

    auto distribute = [&] (auto sink, const TOutputMessageConstPtr& message) {
        auto tracker = TDistributingTracker([] {
        });
        sink->Distribute(message, tracker.AddDestination());
        tracker.Activate();
    };

    std::string failedProducerId;

    {
        auto failedSink = New<TRecoveryTestSink>(context, dynamicSinkContext, storage, router);
        failedSink->Init(stateManager->CreateContext());

        for (i64 i : xrange(messagesCount.BeforeFailPersisted)) {
            distribute(failedSink, messages.at(i));
        }
        doSync(failedSink);
        failedSink->Commit();
        expectedBatchCount += getBatchCount(messagesCount.BeforeFailPersisted);
        EXPECT_EQ(countDistinctSeqNos(), expectedBatchCount);
        failedProducerId = storage->front().second.ProducerId;

        doSync(failedSink);
        failedSink->Commit();
        doSync(failedSink);
        failedSink->Commit();

        for (i64 i : xrange(messagesCount.BeforeFailPersisted, messagesCount.BeforeFail)) {
            distribute(failedSink, messages.at(i));
        }
        doSync(failedSink);
        failedSink->Commit();
        expectedBatchCount += getBatchCount(messagesCount.BeforeFailNotPersisted);
        EXPECT_EQ(countDistinctSeqNos(), expectedBatchCount);
    }

    // New worker modelling a group_by repartition: a fresh producer id, but the
    // deterministic batcher replays the byte-identical not-persisted batch. Blank
    // the persisted producer id so Init mints a new one, keeping the batch bounds.
    {
        auto persisted = stateManager->GetStorage();
        for (auto& [name, value] : persisted) {
            auto node = NYTree::ConvertToNode(value);
            if (node->GetType() == NYTree::ENodeType::Map &&
                node->AsMap()->FindChild("producer_id"))
            {
                node->AsMap()->RemoveChild("producer_id");
                value = NYson::ConvertToYsonString(node);
            }
        }
        stateManager->SetStorage(std::move(persisted));

        auto sink = New<TRecoveryTestSink>(context, dynamicSinkContext, storage, router);
        sink->Init(stateManager->CreateContext());
        for (i64 i : xrange(messagesCount.BeforeFailPersisted, messagesCount.Total)) {
            distribute(sink, messages.at(i));
        }
        doSync(sink);
        expectedBatchCount += getBatchCount(messagesCount.AfterFail);
        sink->Commit();
        EXPECT_EQ(countDistinctSeqNos(), expectedBatchCount);
        doSync(sink);
        sink->Commit();
    }

    std::vector<i64> gotIds;
    for (const auto& [seqNo, batch] : *storage) {
        for (i64 id : batch.Ids) {
            gotIds.push_back(id);
        }
    }
    std::sort(gotIds.begin(), gotIds.end());
    EXPECT_EQ(expectedIds, gotIds);

    std::string newProducerId;
    for (const auto& [seqNo, batch] : *storage) {
        if (batch.ProducerId != failedProducerId) {
            newProducerId = batch.ProducerId;
            break;
        }
    }
    EXPECT_FALSE(newProducerId.empty());
    EXPECT_NE(newProducerId, failedProducerId);

    TClickHouseShardRouter replayRouter(
        ResolveShards(*sinkParameters),
        std::vector<std::string>{"data"},
        {schema});
    THashMap<i64, i64> maxIdBySeqNo;
    for (const auto& [seqNo, batch] : *storage) {
        auto batchMaxId = *std::max_element(batch.Ids.begin(), batch.Ids.end());
        auto& maxId = maxIdBySeqNo[seqNo];
        maxId = std::max(maxId, batchMaxId);
    }

    THashSet<std::pair<i64, std::string>> seenBatchShardPairs;
    for (const auto& [seqNo, batch] : *storage) {
        EXPECT_TRUE(seenBatchShardPairs.emplace(seqNo, batch.ShardName).second);

        for (i64 id : batch.Ids) {
            const auto& shards = replayRouter.GetShards();
            EXPECT_EQ(shards[replayRouter.SelectShard(makeTestMessage(id))].Name, batch.ShardName);
        }

        auto batchBoundToken = std::string(
            TMessageId(LexicographicallySerialize(maxIdBySeqNo[seqNo])).Underlying());
        if (sinkParameters->ShardHosts.empty()) {
            EXPECT_EQ(batch.DedupToken, batchBoundToken);
        } else {
            EXPECT_EQ(batch.DedupToken, batchBoundToken + ":" + batch.ShardName);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseRecoveryTest, TokenStableAcrossRepartition)
{
    CheckRecovery("{host=h;table=t}");
}

TEST(TClickHouseRecoveryTest, ShardedTokenStableAcrossRepartition)
{
    CheckRecovery(R"({shard_hosts={a=["h1"];b=["h2"];c=["h3"]};table=t})");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
