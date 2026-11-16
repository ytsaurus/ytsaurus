#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/transaction_supervisor/transaction_supervisor_service_proxy.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/memory/shared_range.h>

#include <util/random/random.h>

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NChaosClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NTransactionSupervisor;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TTransactionSupervisorClearTest
    : public TApiTestBase
{
protected:
    static TCellId CreateChaosCell()
    {
        auto primaryCellTag = ConvertTo<int>(WaitFor(Client_->GetNode("//sys/@primary_cell_tag"))
            .ValueOrThrow());

        auto bundleAttributes = CreateEphemeralAttributes();
        bundleAttributes->Set("name", "c");
        bundleAttributes->SetYson("chaos_options", BuildYsonStringFluently()
            .BeginMap()
                .Item("peers").BeginList()
                    .Item().BeginMap().EndMap()
                .EndList()
            .EndMap());
        bundleAttributes->SetYson("options", BuildYsonStringFluently()
            .BeginMap()
                .Item("changelog_account").Value("sys")
                .Item("snapshot_account").Value("sys")
                .Item("peer_count").Value(1)
                .Item("independent_peers").Value(true)
                .Item("clock_cluster_tag").Value(primaryCellTag)
            .EndMap());
        TCreateObjectOptions createBundleOptions;
        createBundleOptions.Attributes = std::move(bundleAttributes);
        WaitFor(Client_->CreateObject(EObjectType::ChaosCellBundle, createBundleOptions))
            .ThrowOnError();

        auto cellId = MakeId(
            EObjectType::ChaosCell,
            TCellTag(202),
            RandomNumber<ui64>() % (1ull << 48),
            RandomNumber<ui32>());
        auto cellAttributes = CreateEphemeralAttributes();
        cellAttributes->Set("id", cellId);
        cellAttributes->Set("cell_bundle", "c");
        cellAttributes->Set("area", "default");
        TCreateObjectOptions createCellOptions;
        createCellOptions.Attributes = std::move(cellAttributes);
        auto createdCellId = WaitFor(Client_->CreateObject(EObjectType::ChaosCell, createCellOptions))
            .ValueOrThrow();
        YT_VERIFY(createdCellId == cellId);
        WaitUntilEqual(Format("#%v/@health", cellId), "good");
        return cellId;
    }

    static TReplicationCardId CreateChaosReplicatedTable(TCellId chaosCellId)
    {
        auto cardAttributes = CreateEphemeralAttributes();
        cardAttributes->Set("chaos_cell_id", chaosCellId);
        TCreateObjectOptions createCardOptions;
        createCardOptions.Attributes = std::move(cardAttributes);
        auto cardId = WaitFor(Client_->CreateObject(EObjectType::ReplicationCard, createCardOptions))
            .ValueOrThrow();

        auto tableAttributes = CreateEphemeralAttributes();
        tableAttributes->Set("chaos_cell_bundle", "c");
        tableAttributes->Set("replication_card_id", cardId);
        tableAttributes->SetYson("schema", TYsonString(std::string(
            "[{name=key;type=int64;sort_order=ascending};"
            "{name=value;type=string};]")));
        TCreateNodeOptions createTableOptions;
        createTableOptions.Attributes = std::move(tableAttributes);
        WaitFor(Client_->CreateNode(
            "//tmp/chaos_crt",
            EObjectType::ChaosReplicatedTable,
            createTableOptions))
            .ThrowOnError();

        return cardId;
    }

    static TReplicaId CreateSyncChaosReplica(
        TReplicationCardId cardId,
        const NYPath::TYPath& replicaPath,
        ETableReplicaContentType contentType)
    {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("replication_card_id", cardId);
        attributes->Set("cluster_name", ClusterName_);
        attributes->Set("replica_path", replicaPath);
        attributes->Set("content_type", contentType);
        attributes->Set("mode", ETableReplicaMode::Sync);
        attributes->Set("enabled", true);
        TCreateObjectOptions options;
        options.Attributes = std::move(attributes);
        return WaitFor(Client_->CreateObject(EObjectType::ChaosTableReplica, options))
            .ValueOrThrow();
    }

    static TCellId CreateTabletCell()
    {
        auto cellId = WaitFor(Client_->CreateObject(EObjectType::TabletCell))
            .ValueOrThrow();
        WaitUntilEqual(Format("#%v/@health", cellId), "good");
        return cellId;
    }

    static void CreateTableOnCell(
        const NYPath::TYPath& path,
        TCellId cellId,
        TObjectId upstreamReplicaId = {},
        EObjectType type = EObjectType::Table)
    {
        auto attributes = ConvertToAttributes(NYson::TYsonString(std::string(
            "{dynamic=%true;schema=["
                "{name=key;type=int64;sort_order=ascending};"
                "{name=value;type=string};"
            "];}")));
        if (upstreamReplicaId) {
            attributes->Set("upstream_replica_id", upstreamReplicaId);
        }

        TCreateNodeOptions createOptions;
        createOptions.Attributes = std::move(attributes);
        WaitFor(Client_->CreateNode(path, type, createOptions))
            .ThrowOnError();

        TMountTableOptions mountOptions;
        mountOptions.CellId = cellId;
        WaitFor(Client_->MountTable(path, mountOptions))
            .ThrowOnError();
        WaitUntilEqual(path + "/@tablet_state", "mounted");
    }

    static void WriteRow(const ITransactionPtr& transaction, const NYPath::TYPath& path, i64 key)
    {
        auto nameTable = New<TNameTable>();
        nameTable->GetIdOrRegisterName("key");
        nameTable->GetIdOrRegisterName("value");

        auto rowBuffer = New<TRowBuffer>();
        auto owningRow = YsonToSchemalessRow(Format("<id=0> %v; <id=1> \"value\";", key));
        auto row = rowBuffer->CaptureRow(owningRow.Get());
        auto rows = MakeSharedRange(std::vector<TUnversionedRow>{row}, rowBuffer);
        transaction->WriteRows(path, nameTable, rows);
    }

    static NYPath::TYPath GetCellOrchidPath(TCellId cellId, TStringBuf cellar = "tablet_cells")
    {
        auto address = ConvertTo<std::string>(WaitFor(Client_->GetNode(Format("#%v/@peers/0/address", cellId)))
            .ValueOrThrow());
        return Format("//sys/cluster_nodes/%v/orchid/%v/%v", address, cellar, cellId);
    }

    static bool Exists(const NYPath::TYPath& path)
    {
        return WaitFor(Client_->NodeExists(path))
            .ValueOrThrow();
    }
};

TEST_F(TTransactionSupervisorClearTest, ClearsEagerAndLazyCoordinatorWithoutParticipantResponse)
{
    WaitFor(Client_->SetNode(
        "//sys/accounts/tmp/@resource_limits/tablet_count",
        NYson::ConvertToYsonString(1000)))
        .ThrowOnError();

    auto coordinatorCellId = CreateTabletCell();
    auto participantCellId = CreateTabletCell();
    CreateTableOnCell("//tmp/coordinator", coordinatorCellId);
    CreateTableOnCell("//tmp/participant", participantCellId);

    auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
        .ValueOrThrow();
    auto transactionId = transaction->GetId();
    WriteRow(transaction, "//tmp/coordinator", 1);
    WriteRow(transaction, "//tmp/participant", 2);

    TTransactionCommitOptions commitOptions;
    commitOptions.CoordinatorCellId = coordinatorCellId;
    commitOptions.Force2PC = true;
    commitOptions.CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Eager;
    // The test config lets PrepareTransaction run but keeps participant CommitTransaction queued.
    WaitFor(transaction->Commit(commitOptions))
        .ThrowOnError();

    auto orchidPath = GetCellOrchidPath(coordinatorCellId);
    auto persistentCommitPath = Format(
        "%v/transaction_supervisor/persistent_commits/%v",
        orchidPath,
        transactionId);
    auto transientCommitPath = Format(
        "%v/transaction_supervisor/transient_commits/%v",
        orchidPath,
        transactionId);
    auto managerTransactionPath = Format("%v/transactions/%v", orchidPath, transactionId);

    WaitUntil(
        [&] {
            if (!Exists(persistentCommitPath)) {
                return false;
            }
            auto state = WaitFor(Client_->GetNode(persistentCommitPath + "/persistent_state"))
                .ValueOrThrow();
            return ConvertTo<std::string>(state) == "commit" && !Exists(managerTransactionPath);
        },
        "coordinator did not persist commit and remove local transaction");

    auto commit = ConvertTo<IMapNodePtr>(WaitFor(Client_->GetNode(persistentCommitPath))
        .ValueOrThrow());
    auto participantCellIds = ConvertTo<std::vector<TCellId>>(
        commit->GetChildOrThrow("participant_cell_ids"));
    ASSERT_EQ(participantCellIds.size(), 1u);
    EXPECT_EQ(participantCellIds.front(), participantCellId);
    auto respondedCellIds = commit->GetChildOrThrow("responded_cell_ids")->AsList();
    EXPECT_TRUE(respondedCellIds->GetChildren().empty());

    auto participantTransactionPath = Format(
        "%v/transactions/%v",
        GetCellOrchidPath(participantCellId),
        transactionId);
    EXPECT_TRUE(Exists(participantTransactionPath));

    ASSERT_TRUE(Exists(persistentCommitPath));
    ASSERT_FALSE(Exists(managerTransactionPath));

    auto connection = DynamicPointerCast<NNative::IConnection>(Connection_);
    ASSERT_TRUE(connection);
    auto coordinatorAddress = ConvertTo<std::string>(WaitFor(Client_->GetNode(
        Format("#%v/@peers/0/address", coordinatorCellId)))
        .ValueOrThrow());
    auto channel = NRpc::CreateRealmChannel(
        connection->GetChannelFactory()->CreateChannel(coordinatorAddress),
        coordinatorCellId);
    TTransactionSupervisorServiceProxy proxy(channel);
    auto request = proxy.ClearTransactions();
    NRpc::GenerateMutationId(request);
    ToProto(request->add_transaction_ids(), transactionId);
    WaitFor(request->Invoke())
        .ThrowOnError();

    WaitUntil(
        [&] { return !Exists(persistentCommitPath); },
        "coordinator persistent commit was not cleared");
    EXPECT_FALSE(Exists(transientCommitPath));
    EXPECT_FALSE(Exists(managerTransactionPath));

    auto lazyTransaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
        .ValueOrThrow();
    auto lazyTransactionId = lazyTransaction->GetId();
    WriteRow(lazyTransaction, "//tmp/coordinator", 3);
    WriteRow(lazyTransaction, "//tmp/participant", 4);

    commitOptions.CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Lazy;
    auto lazyCommitFuture = lazyTransaction->Commit(commitOptions);

    auto lazyCommitPath = Format(
        "%v/transaction_supervisor/persistent_commits/%v",
        orchidPath,
        lazyTransactionId);
    auto lazyManagerPath = Format("%v/transactions/%v", orchidPath, lazyTransactionId);
    WaitUntil(
        [&] {
            if (!Exists(lazyCommitPath)) {
                return false;
            }
            auto state = ConvertTo<std::string>(WaitFor(Client_->GetNode(
                lazyCommitPath + "/persistent_state"))
                .ValueOrThrow());
            return state == "commit" && Exists(lazyManagerPath);
        },
        "lazy coordinator did not retain its prepared transaction");

    auto skippedRequest = proxy.ClearTransactions();
    NRpc::GenerateMutationId(skippedRequest);
    ToProto(skippedRequest->add_transaction_ids(), lazyTransactionId);
    WaitFor(skippedRequest->Invoke())
        .ThrowOnError();
    EXPECT_TRUE(Exists(lazyCommitPath));
    EXPECT_TRUE(Exists(lazyManagerPath));

    auto discardRequest = proxy.ClearTransactions();
    NRpc::GenerateMutationId(discardRequest);
    ToProto(discardRequest->add_transaction_ids(), lazyTransactionId);
    discardRequest->set_discard_lazy_transactions(true);
    WaitFor(discardRequest->Invoke())
        .ThrowOnError();

    WaitUntil(
        [&] { return !Exists(lazyCommitPath) && !Exists(lazyManagerPath); },
        "lazy coordinator was not discarded");
    EXPECT_FALSE(WaitFor(lazyCommitFuture).IsOK());
    auto rows = WaitFor(Client_->SelectRows(
        "* from [//tmp/coordinator] where key = 3"))
        .ValueOrThrow();
    EXPECT_TRUE(rows.Rowset->GetRows().empty());
}

TEST_F(TTransactionSupervisorClearTest, ClearsChaosCoordinatorAfterReplicatedTableWrite)
{
    WaitFor(Client_->SetNode(
        "//sys/accounts/tmp/@resource_limits/tablet_count",
        ConvertToYsonString(1000)))
        .ThrowOnError();

    auto chaosCellId = CreateChaosCell();
    auto tabletCellId = CreateTabletCell();
    auto cardId = CreateChaosReplicatedTable(chaosCellId);
    auto dataReplicaId = CreateSyncChaosReplica(
        cardId,
        "//tmp/chaos_replica",
        ETableReplicaContentType::Data);
    auto queueReplicaId = CreateSyncChaosReplica(
        cardId,
        "//tmp/chaos_queue",
        ETableReplicaContentType::Queue);
    CreateTableOnCell("//tmp/chaos_replica", tabletCellId, dataReplicaId);
    CreateTableOnCell(
        "//tmp/chaos_queue",
        tabletCellId,
        queueReplicaId,
        EObjectType::ReplicationLogTable);

    auto chaosOrchidPath = GetCellOrchidPath(chaosCellId, "chaos_cells");
    auto shortcutPath = Format("%v/coordinator_manager/shortcuts/%v", chaosOrchidPath, cardId);
    auto tabletId = ConvertTo<TTabletId>(WaitFor(Client_->GetNode(
        "//tmp/chaos_replica/@tablets/0/tablet_id"))
        .ValueOrThrow());
    WaitUntil(
        [&] {
            auto era = ConvertTo<i64>(WaitFor(Client_->GetNode(Format("#%v/@era", cardId)))
                .ValueOrThrow());
            if (era < 1) {
                return false;
            }
            auto shortcutState = ConvertTo<std::string>(WaitFor(Client_->GetNode(shortcutPath + "/state"))
                .ValueOrThrow());
            auto writeMode = ConvertTo<std::string>(WaitFor(Client_->GetNode(
                Format("#%v/orchid/write_mode", tabletId)))
                .ValueOrThrow());
            auto mountedCard = ConvertTo<IMapNodePtr>(WaitFor(Client_->GetNode(
                Format("#%v/orchid/replication_card", tabletId)))
                .ValueOrThrow());
            auto mountedEra = ConvertTo<i64>(mountedCard->GetChildOrThrow("era"));
            return shortcutState == "granted" && writeMode == "direct" && mountedEra == era;
        },
        "replication card was not ready for direct writes",
        {.Timeout = TDuration::Seconds(120), .IgnoreExceptions = true});

    auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
        .ValueOrThrow();
    auto transactionId = transaction->GetId();
    WriteRow(transaction, "//tmp/chaos_crt", 3);

    TTransactionCommitOptions commitOptions;
    commitOptions.Force2PC = true;
    commitOptions.CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Eager;
    WaitFor(transaction->Commit(commitOptions))
        .ThrowOnError();

    auto persistentCommitPath = Format(
        "%v/transaction_supervisor/persistent_commits/%v",
        chaosOrchidPath,
        transactionId);
    auto transientCommitPath = Format(
        "%v/transaction_supervisor/transient_commits/%v",
        chaosOrchidPath,
        transactionId);
    auto managerTransactionPath = Format("%v/transactions/%v", chaosOrchidPath, transactionId);
    WaitUntil(
        [&] {
            if (!Exists(persistentCommitPath)) {
                return false;
            }
            auto state = ConvertTo<std::string>(WaitFor(Client_->GetNode(
                persistentCommitPath + "/persistent_state"))
                .ValueOrThrow());
            return state == "commit" && !Exists(managerTransactionPath);
        },
        "chaos coordinator did not persist commit and remove local transaction");

    auto commit = ConvertTo<IMapNodePtr>(WaitFor(Client_->GetNode(persistentCommitPath))
        .ValueOrThrow());
    auto participantCellIds = ConvertTo<std::vector<TCellId>>(
        commit->GetChildOrThrow("participant_cell_ids"));
    ASSERT_EQ(participantCellIds.size(), 1u);
    EXPECT_EQ(participantCellIds.front(), tabletCellId);
    EXPECT_TRUE(commit->GetChildOrThrow("responded_cell_ids")->AsList()->GetChildren().empty());

    auto participantTransactionPath = Format(
        "%v/transactions/%v",
        GetCellOrchidPath(tabletCellId),
        transactionId);
    EXPECT_TRUE(Exists(participantTransactionPath));

    auto connection = DynamicPointerCast<NNative::IConnection>(Connection_);
    ASSERT_TRUE(connection);
    auto coordinatorAddress = ConvertTo<std::string>(WaitFor(Client_->GetNode(
        Format("#%v/@peers/0/address", chaosCellId)))
        .ValueOrThrow());
    auto channel = NRpc::CreateRealmChannel(
        connection->GetChannelFactory()->CreateChannel(coordinatorAddress),
        chaosCellId);
    TTransactionSupervisorServiceProxy proxy(channel);
    auto request = proxy.ClearTransactions();
    NRpc::GenerateMutationId(request);
    ToProto(request->add_transaction_ids(), transactionId);
    WaitFor(request->Invoke())
        .ThrowOnError();

    WaitUntil(
        [&] { return !Exists(persistentCommitPath); },
        "chaos coordinator persistent commit was not cleared");
    EXPECT_FALSE(Exists(transientCommitPath));
    EXPECT_FALSE(Exists(managerTransactionPath));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
