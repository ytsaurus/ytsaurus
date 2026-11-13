#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/controller/config.h>
#include <yt/yt/flow/library/cpp/controller/persisted_state_manager.h>

#include <yt/yt/flow/library/cpp/controller/unittests/mock/yt_connector.h>

#include <yt/yt/client/cache/cache.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow::NController {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

using ::testing::_;
using ::testing::Return;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

class TMockClientsCache
    : public NClient::NCache::IClientsCache
{
public:
    MOCK_METHOD(IClientPtr, GetClient, (TStringBuf cluster), (override));
};

////////////////////////////////////////////////////////////////////////////////

class TInputMessagesWatermarkTest
    : public ::testing::Test
{
protected:
    const TIntrusivePtr<StrictMock<TMockClient>> Client_ = New<StrictMock<TMockClient>>();
    const TIntrusivePtr<StrictMock<TMockClient>> ReplicaClient_ = New<StrictMock<TMockClient>>();
    const TIntrusivePtr<StrictMock<TMockClientsCache>> ClientsCache_ = New<StrictMock<TMockClientsCache>>();
    const TIntrusivePtr<StrictMock<TMockYTConnector>> Connector_ = New<StrictMock<TMockYTConnector>>();
    const TPersistedStateManagerConfigPtr Config_ = New<TPersistedStateManagerConfig>();
    IPersistedStateManagerPtr Manager_;

    void SetUp() override
    {
        Config_->Timeout = TDuration::Seconds(7);
        EXPECT_CALL(*Connector_, GetPipelinePath())
            .WillRepeatedly(Return(TRichYPath("<cluster=primary>//pipeline")));
        EXPECT_CALL(*Connector_, GetClient())
            .WillRepeatedly(Return(Client_));
        EXPECT_CALL(*Connector_, GetClientsCache())
            .WillRepeatedly(Return(ClientsCache_));
        Manager_ = CreatePersistedStateManager(Connector_, Config_);
    }

    void Advance(ui64 watermark = 100)
    {
        Manager_->AdvanceInputMessagesWatermark(TSystemTimestamp(watermark));
    }

    TFuture<void> AdvanceAsync()
    {
        return BIND(&IPersistedStateManager::AdvanceInputMessagesWatermark, Manager_, TSystemTimestamp(100))
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    void ExpectMissingCompactTable()
    {
        EXPECT_CALL(*Client_, GetNode("//pipeline/compact_input_messages", _))
            .WillRepeatedly(Return(MakeFuture<TYsonString>(TError(NYTree::EErrorCode::ResolveError, "No such table"))));
    }

    void ExpectPlainTable(const TYPath& path)
    {
        EXPECT_CALL(*Client_, GetNode(path, _))
            .WillRepeatedly([this] (const TYPath& /*path*/, const TGetNodeOptions& options) {
                EXPECT_EQ(options.Timeout, Config_->Timeout);
                return MakeFuture(TYsonString(TYsonStringBuf("<type=table>#")));
            });
    }

    void CheckRuntimeData(const TYsonString& data, const TSetNodeOptions& options, ui64 watermark = 100)
    {
        auto value = ConvertTo<IMapNodePtr>(data)->GetChildOrThrow("watermark")->AsMap();
        EXPECT_EQ(value->GetChildValueOrThrow<std::string>("column_name"), "system_timestamp");
        EXPECT_EQ(value->GetChildValueOrThrow<ui64>("watermark"), watermark);
        EXPECT_EQ(value->GetChildValueOrThrow<std::string>("comparison_operator"), "less");
        EXPECT_EQ(options.Timeout, Config_->Timeout);
    }

    static TYsonString BuildChaosTable(TStringBuf replicas)
    {
        return BuildYsonStringFluently()
            .BeginAttributes()
            .Item("type")
            .Value("chaos_replicated_table")
            .Item("replicas")
            .Value(TYsonStringBuf(replicas))
            .EndAttributes()
            .Entity();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_W(TInputMessagesWatermarkTest, PlainTables)
{
    for (const auto& path : {"//pipeline/input_messages", "//pipeline/compact_input_messages"}) {
        ExpectPlainTable(path);
        EXPECT_CALL(*Client_, SetNode(TYPath(path) + "/@custom_runtime_data", _, _))
            .WillOnce([this] (const TYPath& /*path*/, const TYsonString& data, const TSetNodeOptions& options) {
                CheckRuntimeData(data, options);
                return OKFuture;
            });
    }
    Advance();
}

TEST_W(TInputMessagesWatermarkTest, MissingOptionalTable)
{
    ExpectPlainTable("//pipeline/input_messages");
    ExpectMissingCompactTable();
    EXPECT_CALL(*Client_, SetNode("//pipeline/input_messages/@custom_runtime_data", _, _))
        .WillOnce(Return(OKFuture));
    Advance();
}

TEST_W(TInputMessagesWatermarkTest, UpdatesAllDataReplicasAndSkipsQueues)
{
    for (const auto& name : {"input_messages", "compact_input_messages"}) {
        EXPECT_CALL(*Client_, GetNode(TYPath("//pipeline/") + name, _))
            .WillOnce(Return(MakeFuture(BuildChaosTable(R"({
                data1={content_type=data;cluster_name=remote1;replica_path="//data/messages";mode=sync;state=enabled};
                data2={content_type=data;cluster_name=remote2;replica_path="//data/messages";mode=async;state=disabled};
                log={content_type=queue;cluster_name=queue_cluster;replica_path="//log/messages"};
            })"))));
    }
    auto secondClient = New<StrictMock<TMockClient>>();
    EXPECT_CALL(*ClientsCache_, GetClient("remote1")).Times(2).WillRepeatedly(Return(ReplicaClient_));
    EXPECT_CALL(*ClientsCache_, GetClient("remote2")).Times(2).WillRepeatedly(Return(secondClient));
    for (const auto& client : {ReplicaClient_, secondClient}) {
        EXPECT_CALL(*client, SetNode("//data/messages/@custom_runtime_data", _, _))
            .Times(2)
            .WillRepeatedly([this] (const TYPath& /*path*/, const TYsonString& data, const TSetNodeOptions& options) {
                CheckRuntimeData(data, options);
                return OKFuture;
            });
    }
    Advance();
}

TEST_W(TInputMessagesWatermarkTest, FailedReplicaDoesNotBlockOthersAndIsRetried)
{
    ExpectMissingCompactTable();
    EXPECT_CALL(*Client_, GetNode("//pipeline/input_messages", _))
        .WillRepeatedly(Return(MakeFuture(BuildChaosTable(R"({
            a={content_type=data;cluster_name=remote;replica_path="//data/available"};
            b={content_type=data;cluster_name=remote;replica_path="//data/unavailable"};
        })"))));
    EXPECT_CALL(*ClientsCache_, GetClient("remote")).WillRepeatedly(Return(ReplicaClient_));
    EXPECT_CALL(*ReplicaClient_, SetNode("//data/available/@custom_runtime_data", _, _))
        .Times(2)
        .WillRepeatedly(Return(OKFuture));
    EXPECT_CALL(*ReplicaClient_, SetNode("//data/unavailable/@custom_runtime_data", _, _))
        .WillOnce(Return(MakeFuture<void>(TError("Replica unavailable"))))
        .WillOnce([this] (const TYPath& /*path*/, const TYsonString& data, const TSetNodeOptions& options) {
            CheckRuntimeData(data, options);
            return OKFuture;
        });
    Advance();
    Advance();
}

TEST_W(TInputMessagesWatermarkTest, RediscoversReplicasAtUnchangedWatermark)
{
    ExpectMissingCompactTable();
    EXPECT_CALL(*Client_, GetNode("//pipeline/input_messages", _))
        .WillOnce(Return(MakeFuture(BuildChaosTable(R"({
            a={content_type=data;cluster_name=remote;replica_path="//data/old"};
        })"))))
        .WillOnce(Return(MakeFuture(BuildChaosTable(R"({
            a={content_type=data;cluster_name=remote;replica_path="//data/moved"};
            b={content_type=data;cluster_name=remote;replica_path="//data/new"};
        })"))));
    EXPECT_CALL(*ClientsCache_, GetClient("remote")).WillRepeatedly(Return(ReplicaClient_));
    for (const auto& path : {"//data/old", "//data/moved", "//data/new"}) {
        EXPECT_CALL(*ReplicaClient_, SetNode(TYPath(path) + "/@custom_runtime_data", _, _))
            .WillOnce(Return(OKFuture));
    }
    Advance();
    Advance();
}

TEST_W(TInputMessagesWatermarkTest, StartsReplicaUpdatesBeforeWaitingForReplies)
{
    ExpectMissingCompactTable();
    EXPECT_CALL(*Client_, GetNode("//pipeline/input_messages", _))
        .WillOnce(Return(MakeFuture(BuildChaosTable(R"({
            a={content_type=data;cluster_name=remote;replica_path="//data/first"};
            b={content_type=data;cluster_name=remote;replica_path="//data/second"};
        })"))));
    EXPECT_CALL(*ClientsCache_, GetClient("remote")).WillRepeatedly(Return(ReplicaClient_));
    auto firstStarted = NewPromise<void>();
    auto secondStarted = NewPromise<void>();
    auto firstReply = NewPromise<void>();
    auto secondReply = NewPromise<void>();
    EXPECT_CALL(*ReplicaClient_, SetNode("//data/first/@custom_runtime_data", _, _))
        .WillOnce([firstStarted, firstReply] (const TYPath& /*path*/, const TYsonString& /*data*/, const TSetNodeOptions& /*options*/) {
            firstStarted.TrySet();
            return firstReply.ToFuture();
        });
    EXPECT_CALL(*ReplicaClient_, SetNode("//data/second/@custom_runtime_data", _, _))
        .WillOnce([secondStarted, secondReply] (const TYPath& /*path*/, const TYsonString& /*data*/, const TSetNodeOptions& /*options*/) {
            secondStarted.TrySet();
            return secondReply.ToFuture();
        });

    auto advance = AdvanceAsync();
    auto started = WaitFor(AllSucceeded(std::vector{firstStarted.ToFuture(), secondStarted.ToFuture()})
            .WithTimeout(TDuration::Seconds(5)));
    EXPECT_TRUE(started.IsOK()) << started.GetMessage();
    EXPECT_FALSE(advance.IsSet());
    firstReply.TrySet();
    EXPECT_FALSE(advance.IsSet());
    secondReply.TrySet();
    EXPECT_TRUE(WaitFor(advance.WithTimeout(TDuration::Seconds(5))).IsOK());
}

TEST_W(TInputMessagesWatermarkTest, PublishesOtherTableWhileDiscoveryIsPending)
{
    auto inputStarted = NewPromise<void>();
    auto compactStarted = NewPromise<void>();
    auto inputReply = NewPromise<TYsonString>();
    auto compactReply = NewPromise<TYsonString>();
    auto compactPublished = NewPromise<void>();
    EXPECT_CALL(*Client_, GetNode("//pipeline/input_messages", _))
        .WillOnce([inputStarted, inputReply] (const TYPath& /*path*/, const TGetNodeOptions& /*options*/) {
            inputStarted.TrySet();
            return inputReply.ToFuture();
        });
    EXPECT_CALL(*Client_, GetNode("//pipeline/compact_input_messages", _))
        .WillOnce([compactStarted, compactReply] (const TYPath& /*path*/, const TGetNodeOptions& /*options*/) {
            compactStarted.TrySet();
            return compactReply.ToFuture();
        });
    EXPECT_CALL(*Client_, SetNode("//pipeline/compact_input_messages/@custom_runtime_data", _, _))
        .WillOnce([compactPublished] (const TYPath& /*path*/, const TYsonString& /*data*/, const TSetNodeOptions& /*options*/) {
            compactPublished.TrySet();
            return OKFuture;
        });
    EXPECT_CALL(*Client_, SetNode("//pipeline/input_messages/@custom_runtime_data", _, _))
        .WillOnce(Return(OKFuture));

    auto advance = AdvanceAsync();
    auto started = WaitFor(AllSucceeded(std::vector{inputStarted.ToFuture(), compactStarted.ToFuture()})
            .WithTimeout(TDuration::Seconds(5)));
    EXPECT_TRUE(started.IsOK()) << started.GetMessage();
    compactReply.TrySet(TYsonString(TYsonStringBuf("<type=table>#")));
    EXPECT_TRUE(WaitFor(compactPublished.ToFuture().WithTimeout(TDuration::Seconds(5))).IsOK());
    EXPECT_FALSE(advance.IsSet());
    inputReply.TrySet(TYsonString(TYsonStringBuf("<type=table>#")));
    EXPECT_TRUE(WaitFor(advance.WithTimeout(TDuration::Seconds(5))).IsOK());
}

TEST_W(TInputMessagesWatermarkTest, DiscoveryFailureDoesNotSkipOtherTable)
{
    EXPECT_CALL(*Client_, GetNode("//pipeline/input_messages", _))
        .WillOnce(Return(MakeFuture<TYsonString>(TError("Metadata unavailable"))))
        .WillOnce(Return(MakeFuture(TYsonString(TYsonStringBuf("<type=table>#")))));
    ExpectPlainTable("//pipeline/compact_input_messages");
    EXPECT_CALL(*Client_, SetNode("//pipeline/compact_input_messages/@custom_runtime_data", _, _))
        .Times(2)
        .WillRepeatedly(Return(OKFuture));
    EXPECT_CALL(*Client_, SetNode("//pipeline/input_messages/@custom_runtime_data", _, _))
        .WillOnce(Return(OKFuture));
    Advance();
    Advance();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
