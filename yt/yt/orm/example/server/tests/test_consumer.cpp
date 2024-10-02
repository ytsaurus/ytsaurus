#include "common.h"

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/orm/server/objects/attribute_schema.h>
#include <yt/yt/orm/server/objects/helpers.h>
#include <yt/yt/orm/server/objects/transaction.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>
#include <yt/yt/orm/server/objects/watch_manager.h>

#include <yt/yt/orm/server/master/yt_connector.h>

namespace NYT::NOrm::NExample::NServer::NTests {

using namespace NYT::NYTree;
using namespace NYT::NYson;
using namespace NYT::NOrm::NServer::NMaster;
using namespace NYT::NQueryClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

const TTableSchemaPtr YTConsumerTableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
    TColumnSchema("queue_cluster", EValueType::String, NTableClient::ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("queue_path", EValueType::String, NTableClient::ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("partition_index", EValueType::Uint64, NTableClient::ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("offset", EValueType::Uint64).SetRequired(true),
}, /*strict*/ true, /*uniqueKeys*/ true);

class THelper
{
public:
    THelper(IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Connector_(bootstrap->GetYTConnector())
    , Client_(Connector_->GetClient(Connector_->FormatUserTag()))
    , WatchManager_(bootstrap->GetWatchManager())
    , TransactionManager_(bootstrap->GetTransactionManager())
    { }

    void CreateTable(const TString& tablePath, const NTableClient::TTableSchema& outputSchema, bool isQueueConsumer)
    {
        auto attributes = ::NYT::NYTree::CreateEphemeralAttributes();
        attributes->Set("dynamic", true);
        attributes->Set("schema", outputSchema);
        attributes->Set("treat_as_queue_consumer", isQueueConsumer);

        NApi::TCreateNodeOptions options;
        options.Recursive = true;
        options.IgnoreExisting = true;
        options.Attributes = attributes;

        WaitFor(Client_->CreateNode(tablePath, ::NYT::NObjectClient::EObjectType::Table, options))
            .ThrowOnError();
        WaitFor(Client_->MountTable(tablePath))
            .ThrowOnError();
        WaitTableMounted(tablePath);
    }

    bool IsTableMounted(const TString& tablePath) {
        auto tableInfo = WaitFor(Client_->GetTableMountCache()->GetTableInfo(tablePath))
            .ValueOrThrow();
        return tableInfo->MountedTablets == tableInfo->Tablets;
    }

    void WaitTableMounted(const TString& tablePath)
    {
        WaitForPredicate(
            [&] {return IsTableMounted(tablePath);},
            {.IgnoreExceptions = true});
    }

    void CreateConsumer(const TString& name)
    {
        auto path = Connector_->GetConsumerPath(name);
        CreateTable(path, *YTConsumerTableSchema, /*isQueueConsumer*/ true);
    }

    void EnableTrimming(const TDBTable* queue)
    {
        auto queuePath = Connector_->GetTablePath(queue);
        auto attributes = BuildYsonNodeFluently()
            .BeginMap()
                .Item("auto_trim_config")
                .BeginMap()
                    .Item("enable").Value(true)
                .EndMap()
            .EndMap()->AsMap();

        WaitFor(Client_->MultisetAttributesNode(
            queuePath + "/@",
            attributes))
            .ThrowOnError();
    }

    void ReqisterQueueConsumer(const TDBTable* queue, const TString& consumer, bool vital)
    {
        NYPath::TRichYPath queuePath{Connector_->GetTablePath(queue)};
        NYPath::TRichYPath consumerPath{Connector_->GetConsumerPath(consumer)};
        WaitFor(Client_->RegisterQueueConsumer(queuePath, consumerPath, vital))
            .ThrowOnError();
    }

    void SetQueuesCluster(const std::optional<TString>& cluster)
    {
        auto queuesClusterConfigPath = NYPath::YPathJoin(Connector_->GetMasterPath(),
            "config",
            "watch_manager",
            "queues_cluster");
        if (cluster) {
            auto value = ConvertToYsonString(*cluster);
            WaitFor(Client_->SetNode(
                queuesClusterConfigPath,
                value,
                NApi::TSetNodeOptions{
                    .Recursive = true,
                    .Force = true,
                }))
                .ThrowOnError();
        } else {
            if (WaitFor(Client_->NodeExists(queuesClusterConfigPath)).ValueOrThrow()) {
                WaitFor(Client_->RemoveNode(queuesClusterConfigPath))
                    .ThrowOnError();
            }
        }
    }

    void AdvanceConsumerFromToken(
        const TDBTable* queue,
        const std::optional<TString>& expectedQueueCluster,
        const TString& consumer,
        const TWatchObjectsContinuationToken& token)
    {
        auto transaction = WaitFor(TransactionManager_->StartReadWriteTransaction())
            .ValueOrThrow();
        auto context = transaction->CreateStoreContext();

        WaitForPredicate([&] {
            auto configQueueCluster = WatchManager_->GetConfig()->QueuesCluster;
            return expectedQueueCluster == configQueueCluster;
        });

        for (const auto& offset : token.EventOffsets) {
            context->AdvanceConsumer(consumer,
                queue,
                WatchManager_->GetConfig()->QueuesCluster,
                offset.Tablet,
                /*oldOffset*/ std::nullopt,
                offset.Row);
        }
        context->FillTransaction();
        WaitFor(transaction->Commit())
            .ValueOrThrow();
    }

    TWatchQueryResult WatchWithConsumer(
        TObjectTypeValue type,
        const TString& watchLog,
        const TString& consumer,
        std::optional<TTimestamp> timestamp = std::nullopt)
    {
        TWatchQueryOptions options{
            .ObjectType = type,
            .WatchLog = watchLog,
            .InitialOffsets = TStartFromEarliestOffsetTag{},
            .Consumer = consumer,
            .Timestamp = timestamp,
            .TimeLimit = TDuration::Seconds(30),
            .SkipTrimmed = true,
        };
        return WatchObject(Bootstrap_, options);
    }

    TTimestamp GenerateTimestamp()
    {
        return WaitFor(TransactionManager_->GenerateTimestamps())
            .ValueOrThrow();
    }

    TString GetCluster()
    {
        auto name = Client_->GetClusterName();
        YT_VERIFY(name);
        return TString(name.value());
    }

private:
    IBootstrap* Bootstrap_;
    TYTConnectorPtr Connector_;
    NApi::IClientPtr Client_;
    TWatchManagerPtr WatchManager_;
    TTransactionManagerPtr TransactionManager_;
};

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

TEST(TConsumerTest, TestPullConsumer)
{
    auto bootstrap = GetBootstrap();
    const auto consumer = "test_pull_consumer";
    const auto logName = "watch_log";
    auto logTable = bootstrap->GetWatchManager()->GetLogTableOrCrash(TObjectTypeValues::Author, logName);

    THelper helper(bootstrap);
    ASSERT_ANY_THROW(helper.WatchWithConsumer(TObjectTypeValues::Author, logName, consumer));

    helper.CreateConsumer(consumer);
    ASSERT_ANY_THROW(helper.WatchWithConsumer(TObjectTypeValues::Author, logName, consumer));

    helper.ReqisterQueueConsumer(logTable, consumer, /*vital*/ false);
    ASSERT_NO_THROW(helper.WatchWithConsumer(TObjectTypeValues::Author, logName, consumer));
}

TEST(TConsumerTest, TestAdvanceConsumer)
{
    auto bootstrap = GetBootstrap();
    const auto consumer = "test_advance_consumer";
    const auto logName = "watch_log";
    auto logTable = bootstrap->GetWatchManager()->GetLogTableOrCrash(TObjectTypeValues::Author, logName);

    THelper helper(bootstrap);
    helper.CreateConsumer(consumer);
    helper.ReqisterQueueConsumer(logTable, consumer, /*vital*/ true);
    helper.EnableTrimming(logTable);

    auto generateAndTrimEvent = [&] (const std::optional<TString>& queueCluster) {
        helper.SetQueuesCluster(queueCluster);
        CreateAuthor();
        auto timestamp = helper.GenerateTimestamp();
        auto watchResult = helper.WatchWithConsumer(TObjectTypeValues::Author, logName, consumer, timestamp);
        EXPECT_EQ(1u, watchResult.Events.size());
        helper.AdvanceConsumerFromToken(logTable,
            /*expectedQueueCluster*/ queueCluster,
            consumer,
            watchResult.ContinuationToken);
    };

    auto waitQueueTrimmed = [&] {
        WaitForPredicate([&] {
            auto timestamp = helper.GenerateTimestamp();
            auto watchResult = helper.WatchWithConsumer(TObjectTypeValues::Author, logName, consumer, timestamp);
            return watchResult.Events.size() == 0;
        });
    };


    generateAndTrimEvent(/*queueCluster*/ std::nullopt);
    waitQueueTrimmed();

    generateAndTrimEvent(/*queueCluster*/ helper.GetCluster());
    waitQueueTrimmed();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NTests
