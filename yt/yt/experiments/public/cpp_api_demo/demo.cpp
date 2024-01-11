#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/config.h>

#include <iostream>

using namespace NYT;

using namespace NApi;
using namespace NYPath;
using namespace NYTree;
using namespace NYT::NYson;
using namespace NTableClient;
using namespace NObjectClient;
using namespace NConcurrency;

class Api {
public:
    Api()
    {
        const auto* configPath = std::getenv("YT_CONSOLE_DRIVER_CONFIG_PATH");
        TIFStream configStream(configPath);
        auto config = NYTree::ConvertToNode(&configStream)->AsMap();

        Conn_ = NApi::NNative::CreateConnection(NYTree::ConvertTo<NApi::NNative::TConnectionCompoundConfigPtr>(
            config->GetChildOrThrow("driver")));

        auto clientOptions = NApi::TClientOptions::FromUser(NSecurityClient::RootUserName);
        Client_ = Conn_->CreateClient(clientOptions);

        std::cerr << "Connection created\n";

        CreateTable();
    }

    ~Api()
    {
        WaitFor(Client_->UnmountTable(Table_)).ThrowOnError();
        WaitUntil(Table_ + "/@tablet_state", "unmounted");
        std::cerr << "Table unmounted\n";
        WaitFor(Client_->RemoveNode(TYPath("//tmp/*"))).ThrowOnError();
        std::cerr << "Table removed\n";

        Client_.Reset();
        Conn_.Reset();

        std::cerr << "Gracefully terminated\n";
    }

    void CreateTable()
    {
        auto attributes = ConvertToNode(TYsonString(TStringBuf(
            "{dynamic=%true;schema=["
            "{name=k;type=int64;sort_order=ascending};"
            "{name=v;type=int64};"
            "{name=a;type=any}]}")));

        TCreateNodeOptions options;
        options.Attributes = ConvertToAttributes(attributes);

        WaitFor(Client_->CreateNode(Table_, EObjectType::Table, options))
            .ThrowOnError();
        WaitFor(Client_->MountTable(Table_)).ThrowOnError();
        WaitUntil(Table_ + "/@tablet_state", "mounted");
        std::cerr << "Table created\n";
    }

    void WriteUnversionedRow(
        std::vector<TString> names,
        const TString& rowString)
    {
        auto preparedRow = PrepareUnversionedRow(names, rowString);
        WriteRows(
            std::get<1>(preparedRow),
            std::get<0>(preparedRow));
    }

    void WriteRows(
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TUnversionedRow> rows)
    {
        auto transaction = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();

        transaction->WriteRows(
            Table_,
            nameTable,
            rows);

        auto commitResult = WaitFor(transaction->Commit())
            .ValueOrThrow();
    }

    std::tuple<TSharedRange<TUnversionedRow>, TNameTablePtr> PrepareUnversionedRow(
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
        return std::tuple(MakeSharedRange(rows, std::move(rowBuffer)), std::move(nameTable));
    }

    void WaitUntil(const TYPath& path, const TString& expected)
    {
        auto start = Now();
        bool reached = false;
        for (int attempt = 0; attempt < 2*30; ++attempt) {
            auto state = WaitFor(Client_->GetNode(path))
                .ValueOrThrow();
            auto value = ConvertTo<IStringNodePtr>(state)->GetValue();
            if (value == expected) {
                reached = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(500));
        }

        if (!reached) {
            THROW_ERROR_EXCEPTION("%Qv is not %Qv after %v seconds",
                path,
                expected,
                (Now() - start).Seconds());
        }
    }

    NApi::IConnectionPtr Conn_;
    NApi::IClientPtr Client_;
    const static NYPath::TYPath Table_;

};

const NYPath::TYPath Api::Table_ = "//tmp/t";

__attribute__((destructor))
void foo() {
    puts("__dtor");
}


int main() {
    const auto* configPath = std::getenv("YT_CONSOLE_DRIVER_CONFIG_PATH");
    TIFStream configStream(configPath);
    auto config = NYTree::ConvertToNode(&configStream)->AsMap();
    NLogging::TLogManager::Get()->Configure(
        ConvertTo<NLogging::TLogManagerConfigPtr>(config->GetChildOrThrow("logging")));

    NLogging::TLogger Logger("XXXX");
    YT_LOG_INFO("Trulala");

    auto nameTable = New<TNameTable>();
    auto names = std::vector<TString>{"k", "v", "a"};
    for (const auto& name : names) {
        nameTable->GetIdOrRegisterName(name);
    }

    auto rowBuffer = New<TRowBuffer>();
    TUnversionedRowBuilder b;
    TStringBuf Entity = "#";
    b.AddValue(MakeUnversionedInt64Value(1, 0));
    b.AddValue(MakeUnversionedInt64Value(2, 1));
    b.AddValue(MakeUnversionedAnyValue(TStringBuf(Entity.data(), 1), 2));
    auto v = std::vector<TUnversionedRow>{rowBuffer->CaptureRow(b.GetRow())};
    Api api;
    api.WriteRows(nameTable, MakeSharedRange(v, std::move(rowBuffer)));
}
