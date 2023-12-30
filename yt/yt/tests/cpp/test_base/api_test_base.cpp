#include "api_test_base.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>
#include <yt/yt/ytlib/queue_client/registration_manager.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

namespace NYT::NCppTests {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CppTestsLogger;

////////////////////////////////////////////////////////////////////////////////

void TApiTestBase::SetUpTestCase()
{
    {
        auto configPath = TString(std::getenv("YT_DRIVER_CONFIG_PATH"));
        YT_VERIFY(configPath);
        TIFStream configStream(configPath);
        auto config = ConvertToNode(&configStream)->AsMap();
        auto connection = NApi::CreateConnection(config);

        if (auto nativeConnection = DynamicPointerCast<NNative::IConnection>(connection)) {
            nativeConnection->GetClusterDirectorySynchronizer()->Start();
            nativeConnection->GetQueueConsumerRegistrationManager()->StartSync();
        }

        Connection_ = connection;
    }

    {
        auto configPath = TString(std::getenv("YT_DRIVER_LOGGING_CONFIG_PATH"));
        YT_VERIFY(configPath);
        TIFStream configStream(configPath);
        auto config = ConvertToNode(&configStream)->AsMap();
        NLogging::TLogManager::Get()->Configure(ConvertTo<NLogging::TLogManagerConfigPtr>(config));
    }

    {
        const auto* testSuite = ::testing::UnitTest::GetInstance()->current_test_suite();
        YT_LOG_INFO("Set Up Test (SuiteName: %v)",
            testSuite->name());
    }

    Client_ = CreateClient(NRpc::RootUserName);
    ClusterName_ = ConvertTo<TString>(WaitFor(Client_->GetNode("//sys/@cluster_name")).ValueOrThrow());
}

void TApiTestBase::TearDownTestCase()
{
    {
        const auto* testSuite = ::testing::UnitTest::GetInstance()->current_test_suite();
        YT_LOG_INFO("Tear Down Test (SuiteName: %v)",
            testSuite->name());
    }

    Client_.Reset();
    Connection_.Reset();
}

IClientPtr TApiTestBase::CreateClient(const TString& userName)
{
    auto clientOptions = TClientOptions::FromUser(userName);
    return Connection_->CreateClient(clientOptions);
}

void TApiTestBase::WaitUntil(
    std::function<bool()> predicate,
    const TString& errorMessage)
{
    auto start = Now();
    bool reached = false;
    for (int attempt = 0; attempt < 2*30; ++attempt) {
        if (predicate()) {
            reached = true;
            break;
        }
        Sleep(TDuration::MilliSeconds(500));
    }

    if (!reached) {
        THROW_ERROR_EXCEPTION("%v after %v seconds",
            errorMessage,
            (Now() - start).Seconds());
    }
}

void TApiTestBase::WaitUntilEqual(const TYPath& path, const TString& expected)
{
    WaitUntil(
        [&] {
            auto value = WaitFor(Client_->GetNode(path))
                .ValueOrThrow();
            return ConvertTo<IStringNodePtr>(value)->GetValue() == expected;
        },
        Format("%Qv is not %Qv", path, expected));
}

NApi::IConnectionPtr TApiTestBase::Connection_;
NApi::IClientPtr TApiTestBase::Client_;
TString TApiTestBase::ClusterName_;

////////////////////////////////////////////////////////////////////////////////

void TDynamicTablesTestBase::TearDownTestCase()
{
    SyncUnmountTable(Table_);

    WaitFor(Client_->RemoveNode(TYPath("//tmp/*")))
        .ThrowOnError();

    RemoveTabletCells();
    RemoveSystemObjects("//sys/areas");
    RemoveSystemObjects("//sys/tablet_cell_bundles");

    WaitFor(Client_->SetNode(TYPath("//sys/accounts/tmp/@resource_limits/tablet_count"), ConvertToYsonString(0)))
        .ThrowOnError();

    TApiTestBase::TearDownTestCase();
}

void TDynamicTablesTestBase::SetUpTestCase()
{
    TApiTestBase::SetUpTestCase();

    auto cellId = WaitFor(Client_->CreateObject(EObjectType::TabletCell))
        .ValueOrThrow();
    WaitUntilEqual(TYPath("#") + ToString(cellId) + "/@health", "good");

    WaitFor(Client_->SetNode(TYPath("//sys/accounts/tmp/@resource_limits/tablet_count"), ConvertToYsonString(1000)))
        .ThrowOnError();
}

void TDynamicTablesTestBase::CreateTable(
    const TString& tablePath,
    const TString& schema,
    bool mount)
{
    Table_ = tablePath;
    ASSERT_TRUE(tablePath.StartsWith("//tmp"));

    auto attributes = TYsonString("{dynamic=%true;schema=" + schema + "}");
    TCreateNodeOptions options;
    options.Attributes = ConvertToAttributes(attributes);

    WaitFor(Client_->CreateNode(Table_, EObjectType::Table, options))
        .ThrowOnError();

    if (mount) {
        SyncMountTable(Table_);
    }
}

void TDynamicTablesTestBase::SyncMountTable(const TYPath& path)
{
    WaitFor(Client_->MountTable(path))
        .ThrowOnError();
    WaitUntilEqual(path + "/@tablet_state", "mounted");
}

void TDynamicTablesTestBase::SyncFreezeTable(const TYPath& path)
{
    WaitFor(Client_->FreezeTable(path))
        .ThrowOnError();
    WaitUntilEqual(path + "/@tablet_state", "frozen");
}

void TDynamicTablesTestBase::SyncUnfreezeTable(const TYPath& path)
{
    auto currentTabletStateYson = WaitFor(Client_->GetNode(path + "/@tablet_state"))
        .ValueOrThrow();
    auto currentTabletState = ConvertTo<TString>(currentTabletStateYson);
    YT_VERIFY(currentTabletState == "frozen");

    WaitFor(Client_->UnfreezeTable(path))
        .ThrowOnError();
    WaitUntilEqual(path + "/@tablet_state", "mounted");
}

void TDynamicTablesTestBase::SyncUnmountTable(const TYPath& path)
{
    WaitFor(Client_->UnmountTable(path))
        .ThrowOnError();
    WaitUntilEqual(path + "/@tablet_state", "unmounted");
}

std::tuple<TSharedRange<TUnversionedRow>, TNameTablePtr> TDynamicTablesTestBase::PrepareUnversionedRow(
    const std::vector<TString>& names,
    const TString& rowString)
{
    auto nameTable = New<TNameTable>();
    for (const auto& name : names) {
        nameTable->GetIdOrRegisterName(name);
    }

    auto rowBuffer = New<TRowBuffer>();
    auto owningRow = YsonToSchemalessRow(rowString);
    std::vector<TUnversionedRow> rows{rowBuffer->CaptureRow(owningRow.Get())};
    return std::tuple(MakeSharedRange(std::move(rows), std::move(rowBuffer)), std::move(nameTable));
}

void TDynamicTablesTestBase::WriteUnversionedRow(
    std::vector<TString> names,
    const TString& rowString,
    const IClientPtr& client)
{
    auto preparedRow = PrepareUnversionedRow(names, rowString);
    WriteRows(
        std::get<1>(preparedRow),
        std::get<0>(preparedRow),
        client);
}

void TDynamicTablesTestBase::WriteRows(
    TNameTablePtr nameTable,
    TSharedRange<TUnversionedRow> rows,
    const IClientPtr& client)
{
    auto transaction = WaitFor(client->StartTransaction(NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();

    transaction->WriteRows(
        Table_,
        nameTable,
        rows);

    auto commitResult = WaitFor(transaction->Commit())
        .ValueOrThrow();

    const auto& timestamps = commitResult.CommitTimestamps.Timestamps;
    ASSERT_EQ(1u, timestamps.size());
    ASSERT_EQ(timestamps[0].second, commitResult.PrimaryCommitTimestamp);
}

std::tuple<TSharedRange<TVersionedRow>, TNameTablePtr> TDynamicTablesTestBase::PrepareVersionedRow(
    const std::vector<TString>& names,
    const TString& keyYson,
    const TString& valueYson)
{
    auto nameTable = New<TNameTable>();
    for (const auto& name : names) {
        nameTable->GetIdOrRegisterName(name);
    }

    auto rowBuffer = New<TRowBuffer>();
    auto row = YsonToVersionedRow(rowBuffer, keyYson, valueYson);
    std::vector<TVersionedRow> rows{row};
    return std::tuple(MakeSharedRange(std::move(rows), std::move(rowBuffer)), std::move(nameTable));
}

void TDynamicTablesTestBase::WriteVersionedRow(
    std::vector<TString> names,
    const TString& keyYson,
    const TString& valueYson,
    const IClientPtr& client)
{
    auto preparedRow = PrepareVersionedRow(names, keyYson, valueYson);
    WriteRows(
        std::get<1>(preparedRow),
        std::get<0>(preparedRow),
        client);
}

void TDynamicTablesTestBase::WriteRows(
    TNameTablePtr nameTable,
    TSharedRange<TVersionedRow> rows,
    const IClientPtr& client)
{
    auto transaction = WaitFor(client->StartTransaction(NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();

    transaction->WriteRows(
        Table_,
        nameTable,
        rows);

    TTransactionCommitOptions options{
        .Force2PC = true,
    };
    auto commitResult = WaitFor(transaction->Commit(options))
        .ValueOrThrow();
}

void TDynamicTablesTestBase::RemoveSystemObjects(const TYPath& path)
{
    TListNodeOptions options;
    options.Attributes = {"builtin"};
    auto items = WaitFor(Client_->ListNode(path, options))
         .ValueOrThrow();
    auto itemsList = ConvertTo<IListNodePtr>(items);

    std::vector<TFuture<void>> asyncWait;
    for (const auto& item : itemsList->GetChildren()) {
        const auto& name = item->AsString()->GetValue();
        if (item->Attributes().Get<bool>("builtin", false)) {
            YT_LOG_DEBUG("Do not remove builtin object during teardown (Path: %v, Item: %v)",
                path,
                name);
            continue;
        }
        asyncWait.push_back(Client_->RemoveNode(path + "/" + name));
    }

    WaitFor(AllSucceeded(asyncWait))
        .ThrowOnError();
}

void TDynamicTablesTestBase::RemoveTabletCells(
    std::function<bool(const TString&)> filter)
{
    TYPath path = "//sys/tablet_cells";
    auto items = WaitFor(Client_->ListNode(path))
        .ValueOrThrow();
    auto itemsList = ConvertTo<IListNodePtr>(items);

    std::vector<TTabletCellId> removedCells;
    std::vector<TFuture<void>> asyncWait;
    TRemoveNodeOptions removeOptions;
    removeOptions.Force = true;
    for (const auto& item : itemsList->GetChildren()) {
        const auto& name = item->AsString()->GetValue();
        if (filter(name)) {
            removedCells.push_back(TTabletCellId::FromString(name));
            asyncWait.push_back(Client_->RemoveNode(path + "/" + name, removeOptions));
        }
    }

    WaitFor(AllSucceeded(asyncWait))
        .ThrowOnError();

    WaitUntil(
        [&] {
            auto value = WaitFor(Client_->ListNode(path))
                .ValueOrThrow();
            auto cells = ConvertTo<THashSet<TTabletCellId>>(value);
            for (const auto& cell : removedCells) {
                if (cells.find(cell) != cells.end()) {
                    return false;
                }
            }
            return true;
        },
        "Tablet cells are not removed");
}

TYPath TDynamicTablesTestBase::Table_;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCppTests
