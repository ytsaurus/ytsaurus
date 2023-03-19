#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/dns_resolver.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/library/program/helpers.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/config.h>

namespace NYT {

using namespace NYTree;
using namespace NNet;
using namespace NApi;
using namespace NApi::NRpcProxy;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TConfig)

class TConfig
    : public TSingletonsConfig
{
public:
    NRpcProxy::TConnectionConfigPtr Connection;

    REGISTER_YSON_STRUCT(TConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("connection", &TThis::Connection)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TConfig)

class TRpcProxySampleProgram
    : public TProgram
{
public:
    TRpcProxySampleProgram()
    {
        Opts_.AddLongOption("config").StoreResult(&ConfigFileName_);
        Opts_.AddLongOption("user").StoreResult(&User_);
        Opts_.AddLongOption("token").StoreResult(&Token_);
        Opts_.AddLongOption("sessionid").StoreResult(&SessionId_);
        Opts_.AddLongOption("sslsessionid").StoreResult(&SslSessionId_);
    }

protected:

    bool ValidateSignature(
        const char* command,
        std::vector<const char*>&& signature,
        const TVector<TString>& tokens)
    {
        if (tokens.empty() || tokens.front() != command) {
            return false;
        }
        bool variadic = false;
        if (!signature.empty() && strcmp(signature.back(), "...") == 0) {
            variadic = true;
        }
        bool match = variadic
            ? tokens.size() >= 1 + signature.size()
            : tokens.size() == 1 + signature.size();
        if (!match) {
            Cout << Endl;
            Cout << "ERROR: " << command << " ";
            for (auto atom : signature) {
                Cout << "<" << atom << "> ";
            }
            Cout << Endl;
            return false;
        }
        return true;
    }

    bool ValidateResult(const TError& result)
    {
        if (!result.IsOK()) {
            Cout << Endl;
            Cout << "ERROR: " << ToString(result) << Endl;
            return false;
        }
        return true;
    }

    void HandleTokens(const TVector<TString>& tokens)
    {
        if (ValidateSignature("get", {"path"}, tokens)) {
            auto path = tokens[1];
            auto result = Client_->GetNode(path).Get();
            if (!ValidateResult(result)) return;

            NYson::TYsonWriter writer(&Cout, NYson::EYsonFormat::Pretty);
            NYson::Serialize(result.Value(), &writer);
        }

        if (ValidateSignature("list", {"path"}, tokens)) {
            auto path = tokens[1];
            auto result = Client_->ListNode(path).Get();
            if (!ValidateResult(result)) return;

            NYson::TYsonWriter writer(&Cout, NYson::EYsonFormat::Pretty);
            NYson::Serialize(result.Value(), &writer);
        }

        struct TPrepareRows
        {
            explicit TPrepareRows(const TVector<TString>& tokens)
            {
                auto path = tokens[1];
                TVector<TString> columns;
                Split(tokens[2], ";", columns);
                NameTable = New<TNameTable>();
                RowBuffer = New<TRowBuffer>();
                for (const auto& column : columns) {
                    NameTable->RegisterName(column);
                }


                std::vector<TUnversionedRow> rows;
                for (size_t i = 3; i < tokens.size(); ++i) {
                    auto row = YsonToSchemalessRow(tokens[i]);
                    rows.push_back(RowBuffer->CaptureRow(row));
                }

                Rows = MakeSharedRange(std::move(rows), RowBuffer);
            }

            TNameTablePtr NameTable;
            TRowBufferPtr RowBuffer;
            TSharedRange<TUnversionedRow> Rows;
        };

        if (ValidateSignature("ulookup", {"path", "columns", "..."}, tokens) ||
            ValidateSignature("vlookup", {"path", "columns", "..."}, tokens))
        {
            auto path = tokens[1];
            TPrepareRows prepareRows(tokens);

            if (tokens[0] == "ulookup") {
                auto result = Client_->LookupRows(path, prepareRows.NameTable, prepareRows.Rows).Get();
                if (!ValidateResult(result)) return;

                const auto& rowset = result.Value();
                Cout << "Schema: " << ToString(rowset->GetSchema()) << Endl;
                Cout << "Rows: " << rowset->GetRows().Size() << Endl;
                int i = 0;
                for (const auto& row : rowset->GetRows()) {
                    Cout << '#' << ++i << ": " << ToString(row) << Endl;
                }
            }

            if (tokens[0] == "vlookup") {
                auto result = Client_->VersionedLookupRows(path, prepareRows.NameTable, prepareRows.Rows).Get();
                if (!ValidateResult(result)) return;

                const auto& rowset = result.Value();
                Cout << "Schema: " << ToString(rowset->GetSchema()) << Endl;
                Cout << "Rows: " << rowset->GetRows().Size() << Endl;
                int i = 0;
                for (const auto& row : rowset->GetRows()) {
                    Cout << '#' << ++i << ": " << ToString(row) << Endl;
                }
            }
        }

        if (ValidateSignature("select", {"query", "..."}, tokens)) {
            if (tokens[0] == "select") {
                auto query = JoinToString(tokens.begin() + 1, tokens.end(), TStringBuf(" "));

                Cout << query << Endl;

                auto result = Client_->SelectRows(query).Get();
                if (!ValidateResult(result)) return;

                const auto& rowset = result.Value().Rowset;
                Cout << "Rows: " << rowset->GetRows().Size() << Endl;
                int i = 0;
                for (const auto& row : rowset->GetRows()) {
                    Cout << '#' << ++i << ": " << ToString(row) << Endl;
                }
            }
        }

        if (ValidateSignature("upsert", {"path", "columns", "..."}, tokens)) {
            auto path = tokens[1];
            TPrepareRows prepareRows(tokens);

            auto tx = Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet).Get();
            if (!ValidateResult(tx)) return;

            tx.Value()->WriteRows(path, prepareRows.NameTable, prepareRows.Rows);
            auto result = tx.Value()->Commit().Get();
            if (!ValidateResult(result)) return;

            Cout << "Committed" << Endl;
        }

        if (ValidateSignature("delete", {"path", "columns", "..."}, tokens)) {
            auto path = tokens[1];
            TPrepareRows prepareRows(tokens);

            auto tx = Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet).Get();
            if (!ValidateResult(tx)) return;

            tx.Value()->DeleteRows(path, prepareRows.NameTable, prepareRows.Rows);
            auto result = tx.Value()->Commit().Get();
            if (!ValidateResult(result)) return;

            Cout << "Committed" << Endl;
        }

        if (ValidateSignature("get_in_sync_replicas", {"path", "columns", "..."}, tokens)) {
            auto path = tokens[1];
            TPrepareRows prepareRows(tokens);

            TGetInSyncReplicasOptions options;
            options.Timestamp = Client_
                ->GetTimestampProvider()
                ->GenerateTimestamps(1)
                .Get()
                .ValueOrThrow();
            Cout << "T=" << options.Timestamp << Endl;
            auto result = Client_->GetInSyncReplicas(path, prepareRows.NameTable, prepareRows.Rows, options).Get();
            if (!ValidateResult(result)) return;

            Cout << result.Value().size() << Endl;
            for (const auto& replicas : result.Value()) {
                Cout << "R=" << ToString(replicas) << Endl;
            }
        }
    }

    TConfigPtr LoadConfig()
    {
        auto config = New<TConfig>();

        if (!ConfigFileName_) {
            return config;
        }

        TIFStream stream(ConfigFileName_);
        auto node = ConvertToNode(&stream);
        config->Load(node);
        return config;
    }

    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
        auto config = LoadConfig();

        ConfigureSingletons(config);

        auto connection = NRpcProxy::CreateConnection(config->Connection);

        auto clientOptions = TClientOptions();
        if (User_) {
            clientOptions.User = User_;
        }
        if (!Token_.empty()) {
            clientOptions.Token = Token_;
        }
        if (!SessionId_.empty()) {
            clientOptions.SessionId = SessionId_;
        }
        if (!SslSessionId_.empty()) {
            clientOptions.SslSessionId = SslSessionId_;
        }

        Client_ = connection->CreateClient(clientOptions);

        TString line;
        TVector<TString> tokens;
        for (;;) {
            Cout << Endl;
            Cout << "$> ";
            Cout.Flush();
            if (!Cin.ReadLine(line)) {
                break;
            }
            Split(line, " ", tokens);
            if (tokens.empty()) {
                continue;
            }
            if (tokens[0] == "exit") {
                break;
            }
            HandleTokens(tokens);
            Cout << "\n";
            Cout.Flush();
        }

        Client_.Reset();
    }

private:
    TString ConfigFileName_;
    TString User_;
    TString Token_;
    TString SessionId_;
    TString SslSessionId_;

    IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::TRpcProxySampleProgram().Run(argc, argv);
}
