#include "api_test_base.h"

#include <yt/client/api/rowset.h>
#include <yt/client/api/transaction.h>

#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/name_table.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/logging/config.h>
#include <yt/core/logging/log_manager.h>

#include <yt/core/ypath/token.h>

#include <yt/core/misc/finally.h>

#include <util/system/env.h>

namespace NYT {
namespace NCppTests {

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
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

namespace {

TString GetEnvOrThrow(const TString& name)
{
    auto value = GetEnv(name);
    if (!value) {
        THROW_ERROR_EXCEPTION("%v is not set", name);
    }
    return value;
}

IMapNodePtr ReadConfig(const TString& fileName)
{
    try {
        TIFStream configStream(fileName);
        return ConvertToNode(&configStream)->AsMap();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading configuration file %v",
            fileName);
    }
}

} // namespace

void TApiTestBase::SetUpTestCase()
{
    fprintf(stderr, "SetUpTestCase\n");

    {
        auto configPath = GetEnvOrThrow("YT_CONSOLE_DRIVER_CONFIG_PATH");

        fprintf(stderr, "YT_CONSOLE_DRIVER_CONFIG_PATH = %s\n",
            configPath.c_str());

        auto config = ReadConfig(configPath);
        if (auto logging = config->FindChild("logging")) {
            NLogging::TLogManager::Get()->Configure(ConvertTo<NLogging::TLogManagerConfigPtr>(logging));
        }
    }

    {
        auto configPath = GetEnvOrThrow("YT_DRIVER_CONFIG_PATH_PRIMARY");

        fprintf(stderr, "YT_DRIVER_CONFIG_PATH_PRIMARY = %s\n",
            configPath.c_str());

        auto config = ReadConfig(configPath);
        Connection_ = NApi::CreateConnection(config);
        Client_ = CreateClient(NRpc::RootUserName);
    }

    for (int remoteClusterIndex = 0;; ++remoteClusterIndex) {
        auto configPath = GetEnv("YT_DRIVER_CONFIG_PATH_REMOTE_" + ToString(remoteClusterIndex));
        if (!configPath) {
            break;
        }

        fprintf(stderr, "YT_DRIVER_CONFIG_PATH_REMOTE_%d = %s\n",
            remoteClusterIndex,
            configPath.c_str());

        auto config = ReadConfig(configPath);
        RemoteConnections_.push_back(NApi::CreateConnection(config));
        RemoteClients_.push_back(CreateRemoteClient(remoteClusterIndex, NRpc::RootUserName));
    }
}

void TApiTestBase::TearDownTestCase()
{
    fprintf(stderr, "TearDownTestCase\n");

    Connection_.Reset();
    Client_.Reset();
    RemoteConnections_.clear();
    RemoteClients_.clear();
}

IClientPtr TApiTestBase::CreateClient(const TString& userName)
{
    return Connection_->CreateClient(TClientOptions(userName));
}

IClientPtr TApiTestBase::CreateRemoteClient(int remoteClusterIndex, const TString& userName)
{
    return RemoteConnections_[remoteClusterIndex]->CreateClient(TClientOptions(userName));
}

NApi::IConnectionPtr TApiTestBase::Connection_;
NApi::IClientPtr TApiTestBase::Client_;
std::vector<NApi::IConnectionPtr> TApiTestBase::RemoteConnections_;
std::vector<NApi::IClientPtr> TApiTestBase::RemoteClients_;

////////////////////////////////////////////////////////////////////////////////

void TDynamicTablesTestBase::TearDownTestCase()
{
    auto finallyGuard = Finally([&] {
        TApiTestBase::TearDownTestCase();
    });

    WaitFor(Client_->RemoveNode(TYPath("//tmp/*")))
        .ThrowOnError();

    RemoveTabletCells();

    RemoveUserObjects("//sys/tablet_cell_bundles");

    WaitFor(Client_->SetNode(TYPath("//sys/accounts/tmp/@resource_limits/tablet_count"), ConvertToYsonString(0)))
        .ThrowOnError();
}

void TDynamicTablesTestBase::SetUpTestCase()
{
    TApiTestBase::SetUpTestCase();

    auto cellId = WaitFor(Client_->CreateObject(EObjectType::TabletCell))
        .ValueOrThrow();
    WaitUntilEqual(FromObjectId(cellId) + "/@health", "good");

    WaitFor(Client_->SetNode(TYPath("//sys/accounts/tmp/@resource_limits/tablet_count"), ConvertToYsonString(1000)))
        .ThrowOnError();
}

void TDynamicTablesTestBase::CreateTable(
    const TString& tablePath,
    const TString& schema)
{
    Table_ = tablePath;
    ASSERT_TRUE(tablePath.StartsWith("//tmp"));

    auto attributes = TYsonString("{dynamic=%true;schema=" + schema + "}");
    TCreateNodeOptions options;
    options.Attributes = ConvertToAttributes(attributes);

    WaitFor(Client_->CreateNode(Table_, EObjectType::Table, options))
        .ThrowOnError();

    SyncMountTable(Table_);
}

void TDynamicTablesTestBase::SyncMountTable(const TYPath& path)
{
    WaitFor(Client_->MountTable(path))
        .ThrowOnError();
    WaitUntilEqual(path + "/@tablet_state", "mounted");
}

void TDynamicTablesTestBase::SyncUnmountTable(const TYPath& path)
{
    WaitFor(Client_->UnmountTable(path))
        .ThrowOnError();
    WaitUntilEqual(path + "/@tablet_state", "unmounted");
}

void TDynamicTablesTestBase::WaitUntilEqual(const TYPath& path, const TString& expected)
{
    WaitUntil(
        [&] {
            auto value = WaitFor(Client_->GetNode(path))
                .ValueOrThrow();
            return ConvertTo<IStringNodePtr>(value)->GetValue() == expected;
        },
        Format("%Qv is not %Qv", path, expected));
}

void TDynamicTablesTestBase::WaitUntil(
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
    std::vector<TUnversionedRow> rows{rowBuffer->Capture(owningRow.Get())};
    return std::make_tuple(MakeSharedRange(rows, std::move(rowBuffer)), std::move(nameTable));
}

void TDynamicTablesTestBase::WriteUnversionedRow(
    std::vector<TString> names,
    const TString& rowString,
    const IClientPtr& client)
{
    auto preparedRow = PrepareUnversionedRow(names, rowString);
    WriteUnversionedRows(
        std::get<1>(preparedRow),
        std::get<0>(preparedRow),
        client);
}

void TDynamicTablesTestBase::WriteUnversionedRows(
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
    ASSERT_EQ(1, timestamps.size());
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
    return std::make_tuple(MakeSharedRange(rows, std::move(rowBuffer)), std::move(nameTable));
}

void TDynamicTablesTestBase::WriteVersionedRow(
    std::vector<TString> names,
    const TString& keyYson,
    const TString& valueYson,
    const IClientPtr& client)
{
    auto preparedRow = PrepareVersionedRow(names, keyYson, valueYson);
    WriteVersionedRows(
        std::get<1>(preparedRow),
        std::get<0>(preparedRow),
        client);
}

void TDynamicTablesTestBase::WriteVersionedRows(
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

    auto commitResult = WaitFor(transaction->Commit())
        .ValueOrThrow();
}

void TDynamicTablesTestBase::RemoveUserObjects(const TYPath& path)
{
    TListNodeOptions options;
    options.Attributes = std::vector<TString>{"builtin"};
    auto items = WaitFor(Client_->ListNode(path, options))
         .ValueOrThrow();
    
    auto itemsList = ConvertTo<IListNodePtr>(items);
    std::vector<TFuture<void>> futures;
    for (const auto& item : itemsList->GetChildren()) {
        if (item->Attributes().Get<bool>("builtin")) {
            continue;
        }
        auto name = item->AsString()->GetValue();
        futures.push_back(Client_->RemoveNode(path + "/" + ToYPathLiteral(name)));
    }

    WaitFor(Combine(futures))
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
    for (const auto& item : itemsList->GetChildren()) {
        const auto& name = item->AsString()->GetValue();
        if (filter(name)) {
            removedCells.push_back(TTabletCellId::FromString(name));
            asyncWait.push_back(Client_->RemoveNode(path + "/" + name));
        }
    }

    WaitFor(Combine(asyncWait))
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

} // namespace NCppTests
} // namespace NYT
