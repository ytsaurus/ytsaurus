#include "secrets.h"

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/transaction_supervisor/transaction_participant_service_proxy.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/auth/native_authentication_manager.h>
#include <yt/yt/ytlib/auth/config.h>

#include <yt/yt/library/tvm/service/config.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/api/options.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

#include <util/stream/input.h>
#include <util/string/split.h>
#include <util/system/env.h>
#include <util/system/shellcommand.h>

using namespace NYT;
using namespace NConcurrency;
using namespace NTransactionSupervisor;

namespace {

using TCellTxPair = std::pair<TGuid, TGuid>;

std::vector<TCellTxPair> ParsePairs(int argc, char* argv[])
{
    std::optional<TGuid> cellId;
    std::vector<TGuid> txIds;

    for (int i = 1; i < argc; ++i) {
        if (argv[i] == TStringBuf("--cell-id")) {
            if (i + 1 >= argc) {
                THROW_ERROR_EXCEPTION("Missing value for --cell-id");
            }
            cellId = TGuid::FromString(argv[++i]);
        } else {
            txIds.push_back(TGuid::FromString(argv[i]));
        }
    }

    std::vector<TCellTxPair> pairs;
    if (!txIds.empty()) {
        if (!cellId) {
            THROW_ERROR_EXCEPTION("--cell-id is required when transaction ids are given as arguments");
        }
        for (const auto& txId : txIds) {
            pairs.emplace_back(*cellId, txId);
        }
    } else {
        TString line;
        while (Cin.ReadLine(line)) {
            auto tokens = StringSplitter(line).SplitBySet(" \t").SkipEmpty();
            std::vector<TStringBuf> parts;
            tokens.AddTo(&parts);
            if (parts.size() != 2) {
                THROW_ERROR_EXCEPTION("Expected \"cell_id tx_id\" per line, got %Qv", line);
            }
            pairs.emplace_back(
                TGuid::FromString(parts[0]),
                TGuid::FromString(parts[1]));
        }
    }

    if (pairs.empty()) {
        THROW_ERROR_EXCEPTION("No transaction ids given; pass them as arguments or via stdin");
    }

    return pairs;
}

TString GetLeaderAddress(const NApi::IClientPtr& rpcClient, TGuid cellId)
{
    auto peersYson = WaitFor(rpcClient->GetNode(Format("//sys/tablet_cells/%v/@peers", cellId)))
        .ValueOrThrow();
    auto peersNode = ConvertTo<NYTree::INodePtr>(peersYson);
    for (const auto& peerNode : peersNode->AsList()->GetChildren()) {
        auto peerMap = peerNode->AsMap();
        if (peerMap->GetChildValueOrThrow<TString>("state") == "leading") {
            return peerMap->GetChildValueOrThrow<TString>("address");
        }
    }
    THROW_ERROR_EXCEPTION("No leading peer found for cell %v", cellId);
}

void ConfigureNativeAuthentication(const NApi::IClientPtr& rpcClient, const TString& leaderAddress)
{
    auto tvmServicePath = Format("//sys/cluster_nodes/%v/orchid/config/native_authentication_manager/tvm_service", leaderAddress);

    if (!WaitFor(rpcClient->NodeExists(tvmServicePath)).ValueOrThrow()) {
        Cout << "No native TVM service config at " << tvmServicePath << ", skipping TVM auth" << Endl;
        return;
    }

    Cout << "Configuring tvm auth" << Endl;

    auto tvmServiceYson = WaitFor(rpcClient->GetNode(tvmServicePath))
        .ValueOrThrow();
    auto tvmServiceConfig = ConvertTo<NAuth::TTvmServiceConfigPtr>(tvmServiceYson);

    auto clientSecret = TryGetClientSecret(
        WaitFor(rpcClient->GetClusterName())
            .ValueOrDefault("")
            .value_or(""));

    if (!clientSecret) {
        Cout << "Failed to fetch secret, attempting to read from node" << Endl;

        TShellCommand ssh("ssh");
        auto nodePath = NNet::GetServiceHostName(leaderAddress);
        ssh << (std::string("root@") + std::string(nodePath)) << "cat" << *tvmServiceConfig->ClientSelfSecretPath;
        ssh.Run().Wait();
        if (ssh.GetStatus() != TShellCommand::SHELL_FINISHED || ssh.GetExitCode() != 0) {
            THROW_ERROR_EXCEPTION("Failed to read TVM secret from %v: %v",
                nodePath,
                ssh.GetError());
        }
        clientSecret = ssh.GetOutput();
    }

    auto authConfig = New<NAuth::TNativeAuthenticationManagerConfig>();
    authConfig->TvmService = New<NAuth::TTvmServiceConfig>();
    authConfig->TvmService->ClientSelfId = tvmServiceConfig->ClientSelfId;
    authConfig->TvmService->ClientSelfSecret = *clientSecret;
    authConfig->TvmService->ClientEnableServiceTicketFetching = true;
    authConfig->EnableSubmission = true;
    NAuth::TNativeAuthenticationManager::Get()->Configure(authConfig);
}

} // namespace

int main(int argc, char* argv[])
{
    try {
        std::vector<TCellTxPair> pairs;
        try {
            pairs = ParsePairs(argc, argv);
        } catch (...) {
            Cerr << "Usage: " << argv[0] << " [--cell-id CELL_ID] [...TX IDS]"
                << "\n\tOr feed pairs cell_id tx_id in stdin" << Endl;

            throw;
        }

        auto clusterUrl = GetEnv("YT_PROXY");
        auto rpcConnectionConfig = NApi::NRpcProxy::TConnectionConfig::CreateFromClusterUrl(clusterUrl);
        auto rpcConnection = NApi::NRpcProxy::CreateConnection(rpcConnectionConfig);
        auto rpcClient = rpcConnection->CreateClient(NApi::GetClientOptionsFromEnvStatic());

        THashMap<TGuid, TString> cellToLeader;
        for (const auto& [cellId, txId] : pairs) {
            if (!cellToLeader.contains(cellId)) {
                cellToLeader[cellId] = GetLeaderAddress(rpcClient, cellId);
            }
        }

        ConfigureNativeAuthentication(rpcClient, cellToLeader.begin()->second);

        Cout << "Fetching cluster connection config" << Endl;

        auto clusterConnectionYson = WaitFor(rpcClient->GetNode("//sys/@cluster_connection"))
            .ValueOrThrow();
        auto connectionConfig = ConvertTo<NApi::NNative::TConnectionCompoundConfigPtr>(clusterConnectionYson);
        auto connection = NApi::NNative::CreateConnection(connectionConfig);

        Cout << "Sending aborts" << Endl;

        std::vector<TFuture<void>> futures;
        for (const auto& [cellId, txId] : pairs) {
            auto channel = connection->GetChannelFactory()->CreateChannel(cellToLeader[cellId]);
            auto realmChannel = NRpc::CreateRealmChannel(channel, cellId);

            TTransactionParticipantServiceProxy proxy(realmChannel);
            auto req = proxy.AbortTransaction();
            ToProto(req->mutable_transaction_id(), txId);
            futures.push_back(req->Invoke()
                .AsVoid()
                .Apply(BIND([cellId, txId] {
                    Cerr << Format("Aborted TX % at cell %v\n", txId, cellId);
                })));
        }

        WaitFor(AllSucceeded(futures))
            .ThrowOnError();

        Cout << "Success" << Endl;
    } catch (std::exception& e) {
        Cerr << ToString(TError(e)) << Endl;
    }

    return 0;
}
