#include <yt/yt/client/hedging/cache.h>
#include <yt/yt/client/hedging/counter.h>
#include <yt/yt/client/hedging/hedging.h>
#include <yt/yt_proto/yt/client/hedging/proto/config.pb.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/actions/cancelable_context.h>

#include <library/cpp/iterator/zip.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/vector.h>

namespace NYT::NHedgingClient::NRpc {

using ::testing::_;
using ::testing::Return;
using ::testing::StrictMock;

using TStrictMockClient = StrictMock<NApi::TMockClient>;

namespace {

    const auto SleepQuantum = TDuration::MilliSeconds(100);
    const auto CheckPeriod = TDuration::Seconds(1);

    class TMockClientsCache : public IClientsCache {
    public:
        MOCK_METHOD(NApi::IClientPtr, GetClient, (TStringBuf url), (override));
    };

    NApi::IClientPtr CreateTestHedgingClient(TDuration banPenalty, TDuration banDuration,
                                                  std::initializer_list<NApi::IClientPtr> clients,
                                                  std::initializer_list<TDuration> initialPenalties = {TDuration::Zero(), SleepQuantum},
                                                  const IPenaltyProviderPtr& penaltyProvider = CreateDummyPenaltyProvider()) {
        THedgingClientOptions options;
        options.BanPenalty = banPenalty;
        options.BanDuration = banDuration;
        size_t clientId = 0;
        for (auto [client, initialPenalty] : Zip(clients, initialPenalties)) {
            auto currCliendId = "seneca-" + ToString(++clientId);
            options.Clients.emplace_back(client, currCliendId, initialPenalty, New<TCounter>(currCliendId));
        }
        return CreateHedgingClient(options, penaltyProvider);
    }

    IPenaltyProviderPtr CreateReplicationLagPenaltyProvider(
            const NYPath::TYPath& path,
            const TString& cluster,
            TDuration maxTabletLag,
            TDuration lagPenalty,
            NApi::IClientPtr masterClient,
            const bool clearPenaltiesOnErrors = false,
            const TDuration checkPeriod = CheckPeriod) {
        TReplicaionLagPenaltyProviderConfig config;

        config.SetTablePath(path);
        config.AddReplicaClusters(cluster);
        config.SetMaxTabletsWithLagFraction(0.5);
        config.SetMaxTabletLag(maxTabletLag.Seconds());
        config.SetCheckPeriod(checkPeriod.Seconds());
        config.SetLagPenalty(lagPenalty.MilliSeconds());
        config.SetClearPenaltiesOnErrors(clearPenaltiesOnErrors);

        return CreateReplicaionLagPenaltyProvider(config, masterClient);
    }

} // namespace

// Using LinkNode method for testing because it's return value is YsonString.
// It makes easier to check from which client result has come from just by comparing corresponding string values.
TEST(THedgingClientTest, GetResultFromClientWithMinEffectivePenalty) {
    NYPath::TYPath path = "/test/1234";

    NYson::TYsonString firstClientResult(TStringBuf("FirstClientData"));

    auto firstMockClient = New<TStrictMockClient>();
    auto secondMockClient = New<TStrictMockClient>();

    EXPECT_CALL(*firstMockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture(firstClientResult)));
    EXPECT_CALL(*secondMockClient, ListNode(path, _)).Times(0);
    auto client = CreateTestHedgingClient(SleepQuantum * 2, SleepQuantum,
                                         {firstMockClient, secondMockClient});
    auto queryResult = NConcurrency::WaitFor(client->ListNode(path));

    // Check that query result is from first client, because it's effective initial penalty is minimal.
    EXPECT_TRUE(queryResult.IsOK());
    EXPECT_EQ(queryResult.Value().AsStringBuf(), firstClientResult.AsStringBuf());
}

TEST(THedgingClientTest, GetSecondClientResultWhenFirstClientHasFailed) {
    NYPath::TYPath path = "/test/1234";

    NYson::TYsonString firstClientResult(TStringBuf("FirstClientData"));
    NYson::TYsonString secondClientResult(TStringBuf("SecondClientData"));

    auto firstMockClient = New<TStrictMockClient>();
    auto secondMockClient = New<TStrictMockClient>();

    EXPECT_CALL(*firstMockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));
    EXPECT_CALL(*secondMockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture(secondClientResult)));

    auto client = CreateTestHedgingClient(SleepQuantum * 2, SleepQuantum * 2,
                                          {firstMockClient, secondMockClient});

    auto queryResult = NConcurrency::WaitFor(client->ListNode(path));

    // Check that query result is from second client, because first client returned failure and got banned.
    EXPECT_TRUE(queryResult.IsOK());
    EXPECT_EQ(queryResult.Value().AsStringBuf(), secondClientResult.AsStringBuf());
}

TEST(THedgingClientTest, GetFirstClientResultAfterBanTimeHasElapsed) {
    NYPath::TYPath path = "/test/1234";

    NYson::TYsonString firstClientResult(TStringBuf("FirstClientData"));
    NYson::TYsonString secondClientResult(TStringBuf("SecondClientData"));

    auto firstMockClient = New<TStrictMockClient>();
    auto secondMockClient = New<TStrictMockClient>();

    EXPECT_CALL(*firstMockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))))
        .WillOnce(Return(MakeFuture(firstClientResult)));
    EXPECT_CALL(*secondMockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture(secondClientResult)));

    auto banDuration = SleepQuantum * 2;
    auto client = CreateTestHedgingClient(banDuration, banDuration,
                                          {firstMockClient, secondMockClient});
    auto firstQueryResult = NConcurrency::WaitFor(client->ListNode(path));

    // Check that first query result is from second client, because first client returned failure and got banned.
    EXPECT_TRUE(firstQueryResult.IsOK());
    EXPECT_EQ(firstQueryResult.Value().AsStringBuf(), secondClientResult.AsStringBuf());

    NConcurrency::TDelayedExecutor::WaitForDuration(banDuration);
    auto secondQueryResult = NConcurrency::WaitFor(client->ListNode(path));

    // Check that second query result is from first client, because ban time has elapsed and it's effective initial penalty is minimal again.
    EXPECT_TRUE(secondQueryResult.IsOK());
    EXPECT_EQ(secondQueryResult.Value().AsStringBuf(), firstClientResult.AsStringBuf());
}

TEST(THedgingClientTest, GetSecondClientResultWhenFirstClientIsBanned) {
    NYPath::TYPath path = "/test/1234";

    NYson::TYsonString firstClientResult(TStringBuf("FirstClientData"));
    NYson::TYsonString secondClientResult(TStringBuf("SecondClientData"));

    auto firstMockClient = New<TStrictMockClient>();
    auto secondMockClient = New<TStrictMockClient>();

    EXPECT_CALL(*firstMockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));
    EXPECT_CALL(*secondMockClient, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(secondClientResult)));

    auto client = CreateTestHedgingClient(SleepQuantum * 2, TDuration::Seconds(2),
                                          {firstMockClient, secondMockClient});
    auto firstQueryResult = NConcurrency::WaitFor(client->ListNode(path));

    // Check that first query result is from second client, because first client returned failure and got banned.
    EXPECT_TRUE(firstQueryResult.IsOK());
    EXPECT_EQ(firstQueryResult.Value().AsStringBuf(), secondClientResult.AsStringBuf());

    auto secondQueryResult = NConcurrency::WaitFor(client->ListNode(path));

    // Check that second query result is from second client, because first client is still banned.
    EXPECT_TRUE(secondQueryResult.IsOK());
    EXPECT_EQ(secondQueryResult.Value().AsStringBuf(), secondClientResult.AsStringBuf());
}

TEST(THedgingClientTest, GetSecondClientResultWhenFirstClientIsSleeping) {
    NYPath::TYPath path = "/test/1234";

    NYson::TYsonString firstClientResult(TStringBuf("FirstClientData"));
    NYson::TYsonString secondClientResult(TStringBuf("SecondClientData"));

    auto firstMockClient = New<TStrictMockClient>();
    auto secondMockClient = New<TStrictMockClient>();

    EXPECT_CALL(*firstMockClient, ListNode(path, _))
        .WillOnce(Return(NConcurrency::TDelayedExecutor::MakeDelayed(TDuration::Seconds(2)).Apply(BIND([=]() { return firstClientResult; }))));
    EXPECT_CALL(*secondMockClient, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(secondClientResult)));

    auto client = CreateTestHedgingClient(SleepQuantum * 2, SleepQuantum,
                                          {firstMockClient, secondMockClient});
    auto queryResult = NConcurrency::WaitFor(client->ListNode(path));

    // Check that query result is from second client, because first client is sleeping.
    EXPECT_TRUE(queryResult.IsOK());
    EXPECT_EQ(queryResult.Value().AsStringBuf(), secondClientResult.AsStringBuf());
}

TEST(THedgingClientTest, FirstClientIsBannedBecauseResponseWasCancelled) {
    NYPath::TYPath path = "/test/1234";

    NYson::TYsonString firstClientResult(TStringBuf("FirstClientData"));
    NYson::TYsonString secondClientResult(TStringBuf("SecondClientData"));

    auto firstMockClient = New<TStrictMockClient>();
    auto secondMockClient = New<TStrictMockClient>();

    EXPECT_CALL(*firstMockClient, ListNode(path, _))
        .WillOnce(Return(NConcurrency::TDelayedExecutor::MakeDelayed(SleepQuantum * 2).Apply(BIND([=]() { return firstClientResult; }))))
        .WillRepeatedly(Return(MakeFuture(firstClientResult)));
    EXPECT_CALL(*secondMockClient, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(secondClientResult)));

    auto client = CreateTestHedgingClient(SleepQuantum * 2, TDuration::Seconds(2),
                                          {firstMockClient, secondMockClient});
    auto firstQueryResult = NConcurrency::WaitFor(client->ListNode(path));

    // Check that query result is from second client, because first client is sleeping.
    EXPECT_TRUE(firstQueryResult.IsOK());
    EXPECT_EQ(firstQueryResult.Value().AsStringBuf(), secondClientResult.AsStringBuf());

    // Wait for finish of all requests
    Sleep(SleepQuantum);

    auto secondQueryResult = NConcurrency::WaitFor(client->ListNode(path));

    // Check that second query result is from second client, because first client was cancelled and got banned.
    EXPECT_TRUE(secondQueryResult.IsOK());
    EXPECT_EQ(secondQueryResult.Value().AsStringBuf(), secondClientResult.AsStringBuf());
}

TEST(THedgingClientTest, AmnestyBanPenaltyIfClientSucceeded) {
    NYPath::TYPath path = "/test/1234";

    NYson::TYsonString firstClientResult(TStringBuf("FirstClientData"));
    NYson::TYsonString secondClientResult(TStringBuf("SecondClientData"));
    NYson::TYsonString thirdClientResult(TStringBuf("ThirdClientData"));

    auto firstMockClient = New<TStrictMockClient>();
    auto secondMockClient = New<TStrictMockClient>();
    auto thirdMockClient = New<TStrictMockClient>();

    EXPECT_CALL(*firstMockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))))
        .WillRepeatedly(Return(MakeFuture(firstClientResult)));
    EXPECT_CALL(*secondMockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture(secondClientResult)))
        .WillOnce(Return(NConcurrency::TDelayedExecutor::MakeDelayed(TDuration::Seconds(100)).Apply(BIND([=]() { return secondClientResult; }))))
        .WillRepeatedly(Return(MakeFuture(secondClientResult)));
    EXPECT_CALL(*thirdMockClient, ListNode(path, _))
        .WillRepeatedly(Return(NConcurrency::TDelayedExecutor::MakeDelayed(TDuration::Seconds(100)).Apply(BIND([=]() { return thirdClientResult; }))));

    auto client = CreateTestHedgingClient(SleepQuantum * 2, TDuration::Seconds(30),
                                          {firstMockClient, secondMockClient, thirdMockClient},
                                          {TDuration::Zero(), SleepQuantum, SleepQuantum * 2});
    auto firstQueryResult = NConcurrency::WaitFor(client->ListNode(path));

    // Check that query result is from second client, because first client finished with an error.
    EXPECT_TRUE(firstQueryResult.IsOK());
    EXPECT_EQ(firstQueryResult.Value().AsStringBuf(), secondClientResult.AsStringBuf());

    // Wait for finish of all requests
    Sleep(SleepQuantum * 2);

    auto secondQueryResult = NConcurrency::WaitFor(client->ListNode(path));

    // Check that second query result is from first client, because other clients were sleeping.
    EXPECT_TRUE(secondQueryResult.IsOK());
    EXPECT_EQ(secondQueryResult.Value().AsStringBuf(), firstClientResult.AsStringBuf());

    // Wait for finish of all requests
    Sleep(SleepQuantum * 2);

    auto thirdQueryResult = NConcurrency::WaitFor(client->ListNode(path));

    // Check that third query result is from first client again, because it's penalty was amnestied.
    EXPECT_TRUE(thirdQueryResult.IsOK());
    EXPECT_EQ(thirdQueryResult.Value().AsStringBuf(), firstClientResult.AsStringBuf());
}

TEST(THedgingClientTest, MultiThread) {
    NYPath::TYPath path = "/test/1234";

    auto firstMockClient = New<TStrictMockClient>();
    auto secondMockClient = New<TStrictMockClient>();
    NYson::TYsonString firstClientResult(TStringBuf("FirstClientData"));
    NYson::TYsonString secondClientResult(TStringBuf("SecondClientData"));

    EXPECT_CALL(*firstMockClient, ListNode(path, _)).WillRepeatedly([=](const NYPath::TYPath&, const NApi::TListNodeOptions& options) {
        if (options.Timeout) {
            return NConcurrency::TDelayedExecutor::MakeDelayed(*options.Timeout).Apply(BIND([=]() {
                return firstClientResult;
            }));
        }
        return MakeFuture(firstClientResult);
    });
    EXPECT_CALL(*secondMockClient, ListNode(path, _)).WillRepeatedly(Return(MakeFuture(secondClientResult)));

    auto client = CreateTestHedgingClient(TDuration::MilliSeconds(1), SleepQuantum,
                                          {firstMockClient, secondMockClient},
                                          {SleepQuantum, SleepQuantum * 3});

    auto tp = New<NConcurrency::TThreadPool>(10, "test");
    TVector<TFuture<void>> futures(Reserve(100));
    for (int i = 0; i < 100; ++i) {
        futures.emplace_back(BIND([=]() {
            for (int j = 0; j < 100; ++j) {
                NApi::TListNodeOptions options;
                // on each 5-th request for 1-st and 2-nd thread, the first client will timeout
                if (i < 2 && (0 == j % 5)) {
                    options.Timeout = TDuration::Seconds(2);
                }
                auto v = NConcurrency::WaitFor(client->ListNode(path, options)).ValueOrThrow();
                if (options.Timeout) {
                    EXPECT_EQ(secondClientResult.AsStringBuf(), v.AsStringBuf());
                } else {
                    EXPECT_EQ(firstClientResult.AsStringBuf(), v.AsStringBuf());
                }
            }
        }).AsyncVia(tp->GetInvoker()).Run());
    }

    for (auto& f : futures) {
        EXPECT_NO_THROW(f.Get().ThrowOnError());
    }
}

TEST(THedgingClientTest, ResponseFromSecondClientWhenFirstHasReplicationLag) {
    NYPath::TYPath path = "/test/1234";

    auto firstMockClient = New<TStrictMockClient>();
    auto secondMockClient = New<TStrictMockClient>();
    NYson::TYsonString firstClientResult(TStringBuf("FirstClientData"));
    NYson::TYsonString secondClientResult(TStringBuf("SecondClientData"));

    EXPECT_CALL(*firstMockClient, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(firstClientResult)));
    EXPECT_CALL(*secondMockClient, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(secondClientResult)));

    auto client = CreateTestHedgingClient(SleepQuantum * 2, SleepQuantum,
                                         {firstMockClient, secondMockClient});
    auto queryResult = NConcurrency::WaitFor(client->ListNode(path));

    // Check that query result is from first client, because it's effective initial penalty is minimal.
    EXPECT_TRUE(queryResult.IsOK());
    EXPECT_EQ(queryResult.Value().AsStringBuf(), firstClientResult.AsStringBuf());

    TString cluster = "seneca-1";
    auto maxTabletLag = TDuration::Seconds(10);
    auto lagPenalty = 2 * SleepQuantum;

    NYson::TYsonString replicasResult(TStringBuf("{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-1\"}}"));
    NYson::TYsonString tabletCountResult(TStringBuf("1"));

    auto masterClient = New<TStrictMockClient>();

    EXPECT_CALL(*masterClient, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    EXPECT_CALL(*masterClient, GetNode(path + "/@tablet_count", _))
        .WillRepeatedly(Return(MakeFuture(tabletCountResult)));

    std::vector<NApi::TTabletInfo> tabletInfos(1);
    tabletInfos[0].TableReplicaInfos = std::make_optional(std::vector<NApi::TTabletInfo::TTableReplicaInfo>());
    auto& replicaTabletsInfo = tabletInfos[0].TableReplicaInfos->emplace_back();
    replicaTabletsInfo.ReplicaId = NTabletClient::TTableReplicaId::FromString("575f-131-40502c5-201b420f");
    replicaTabletsInfo.LastReplicationTimestamp = NTransactionClient::TimestampFromUnixTime(
        TInstant::Now().Seconds() - 2 * maxTabletLag.Seconds());

    EXPECT_CALL(*masterClient, GetTabletInfos(path, _, _))
        .WillRepeatedly(Return(MakeFuture(tabletInfos)));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(
        path, cluster, maxTabletLag, lagPenalty, masterClient);
    Sleep(2 * CheckPeriod);

    auto clientWithPenaltyProvider = CreateTestHedgingClient(TDuration::MilliSeconds(1), SleepQuantum,
                                                            {firstMockClient, secondMockClient},
                                                            {TDuration::Zero(), SleepQuantum},
                                                            PenaltyProviderPtr);

    auto queryResultWithReplicationLagPolicy = NConcurrency::WaitFor(clientWithPenaltyProvider->ListNode(path));

    // Check that query result is from second client, because first client recieved penalty updater because of replication lag.
    EXPECT_TRUE(queryResultWithReplicationLagPolicy.IsOK());
    EXPECT_EQ(queryResultWithReplicationLagPolicy.Value().AsStringBuf(), secondClientResult.AsStringBuf());
}

TEST(THedgingClientTest, CreatingHedgingClientWithPreinitializedClients) {
    const TString clusterName = "test_cluster";
    NYPath::TYPath path = "/test/1234";
    NYson::TYsonString clientResult(TStringBuf("ClientData"));

    auto mockClient = New<TStrictMockClient>();
    EXPECT_CALL(*mockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture(clientResult)));

    auto mockClientsCache = New<StrictMock<TMockClientsCache>>();
    EXPECT_CALL(*mockClientsCache, GetClient(clusterName)).WillOnce(Return(mockClient));

    THedgingClientConfig hedgingClientConfig;
    hedgingClientConfig.SetBanDuration(100);
    hedgingClientConfig.SetBanPenalty(200);
    auto clientOptions = hedgingClientConfig.AddClients();
    clientOptions->SetInitialPenalty(0);
    clientOptions->MutableClientConfig()->SetClusterName(clusterName);

    auto hedgingClient = CreateHedgingClient(hedgingClientConfig, mockClientsCache);

    auto queryResult = NConcurrency::WaitFor(hedgingClient->ListNode(path));

    // Check that query result is from preinitialized client.
    EXPECT_TRUE(queryResult.IsOK());
    EXPECT_EQ(queryResult.Value().AsStringBuf(), clientResult.AsStringBuf());
}

TEST(THedgingClientTest, ResponseFromFirstClientWhenReplicationLagUpdaterFails) {
    NYPath::TYPath path = "/test/1234";

    auto firstMockClient = New<TStrictMockClient>();
    auto secondMockClient = New<TStrictMockClient>();
    NYson::TYsonString firstClientResult(TStringBuf("FirstClientData"));
    NYson::TYsonString secondClientResult(TStringBuf("SecondClientData"));

    EXPECT_CALL(*firstMockClient, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(firstClientResult)));
    EXPECT_CALL(*secondMockClient, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(secondClientResult)));

    TString cluster = "seneca-1";
    auto maxTabletLag = TDuration::Seconds(10);
    auto lagPenalty = 2 * SleepQuantum;

    NYson::TYsonString replicasResult(TStringBuf("{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-1\"}}"));
    NYson::TYsonString tabletCountResult(TStringBuf("1"));

    auto masterClient = New<TStrictMockClient>();

    EXPECT_CALL(*masterClient, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    EXPECT_CALL(*masterClient, GetNode(path + "/@tablet_count", _))
        .WillOnce(Return(MakeFuture(tabletCountResult)))
        .WillRepeatedly(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));

    std::vector<NApi::TTabletInfo> tabletInfos(1);
    tabletInfos[0].TableReplicaInfos = std::make_optional(std::vector<NApi::TTabletInfo::TTableReplicaInfo>());
    auto& replicaTabletsInfo = tabletInfos[0].TableReplicaInfos->emplace_back();
    replicaTabletsInfo.ReplicaId = NTabletClient::TTableReplicaId::FromString("575f-131-40502c5-201b420f");
    replicaTabletsInfo.LastReplicationTimestamp = NTransactionClient::TimestampFromUnixTime(TInstant::Now().Seconds() - 2 * maxTabletLag.Seconds());

    EXPECT_CALL(*masterClient, GetTabletInfos(path, _, _))
        .WillRepeatedly(Return(MakeFuture(tabletInfos)));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(path, cluster, maxTabletLag, lagPenalty, masterClient, true, 2 * CheckPeriod);
    Sleep(CheckPeriod);

    auto clientWithPenaltyProvider = CreateTestHedgingClient(TDuration::MilliSeconds(1), SleepQuantum,
                                                            {firstMockClient, secondMockClient},
                                                            {TDuration::Zero(), SleepQuantum},
                                                            PenaltyProviderPtr);

    auto queryResultWithReplicationLagPolicy = NConcurrency::WaitFor(clientWithPenaltyProvider->ListNode(path));

    // Check that query result is from second client, because first client recieved penalty because of replication lag.
    EXPECT_TRUE(queryResultWithReplicationLagPolicy.IsOK());
    EXPECT_EQ(queryResultWithReplicationLagPolicy.Value().AsStringBuf(), secondClientResult.AsStringBuf());

    Sleep(2 * CheckPeriod);
    auto queryResultWithCleanedPenalty = NConcurrency::WaitFor(clientWithPenaltyProvider->ListNode(path));

    // Check that query result is from first client, because replication lag was cleaned.
    EXPECT_TRUE(queryResultWithCleanedPenalty.IsOK());
    EXPECT_EQ(queryResultWithCleanedPenalty.Value().AsStringBuf(), firstClientResult.AsStringBuf());
}

} // namespace NYT::NHedgingClient::NRpc
