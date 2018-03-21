#include "yt_connector.h"
#include "bootstrap.h"
#include "config.h"
#include "private.h"

#include <yp/server/objects/db_schema.h>

#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/query_client/functions.h>
#include <yt/ytlib/query_client/functions_cache.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/actions/cancelable_context.h>

#include <yt/core/ypath/token.h>

#include <yt/core/ytree/ephemeral_node_factory.h>

namespace NYP {
namespace NServer {
namespace NMaster {

using namespace NYT::NApi;
using namespace NYT::NCypressClient;
using namespace NYT::NObjectClient;
using namespace NYT::NQueryClient;
using namespace NYT::NTransactionClient;
using namespace NYT::NYPath;
using namespace NYT::NConcurrency;
using namespace NYT::NYPath;
using namespace NYT::NYTree;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

class TYTConnector::TImpl
    : public TRefCounted
{
public:
    TImpl(TBootstrap* bootstrap, TYTConnectorConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , MasterDiscoveryExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::OnMasterDiscovery, MakeWeak(this)),
            Config_->MasterDiscoveryPeriod))
    { }

    void Initialize()
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetControlInvoker(), ControlThread);

        DBPath_= GetRootPath() + "/db";

        LOG_INFO("DB initialized (Path: %v)",
            DBPath_);

        Connection_ = CreateNativeConnection(Config_->Connection);
        Client_ = Connection_->CreateNativeClient(TClientOptions(Config_->User));

        MasterDiscoveryExecutor_->Start();

        ScheduleConnect(true);
    }


    const INativeClientPtr& GetClient()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Client_;
    }

    const TTypeInferrerMapPtr& GetTypeInferrers()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return TypeInferrers_;
    }

    const TFunctionProfilerMapPtr& GetFunctionProfilers()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return FunctionProfilers_;
    }

    const TYPath& GetRootPath()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Config_->RootPath;
    }

    const TYPath& GetDBPath()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return DBPath_;
    }

    TYPath GetTablePath(const NObjects::TDbTable* table)
    {
        return DBPath_ + "/" + ToYPathLiteral(table->Name);
    }

    TClusterTag GetClusterTag()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Config_->ClusterTag;
    }

    TMasterInstanceTag GetInstanceTag()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Config_->InstanceTag;
    }


    bool IsConnected()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return IsConnected_.load(std::memory_order_relaxed);
    }

    bool IsLeading()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return IsLeading_.load(std::memory_order_relaxed);
    }

    const ITransactionPtr& GetInstanceLockTransaction()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        return InstanceLockTransaction_;
    }

    std::vector<TMasterDiscoveryInfo> GetMasters()
    {
        TReaderGuard guard(MasterDiscoveryLock_);
        return MasterDiscoveryInfos_;
    }

    DEFINE_SIGNAL(void(), Connected);
    DEFINE_SIGNAL(void(), Disconnected);
    DEFINE_SIGNAL(void(), ValidateConnection);

    DEFINE_SIGNAL(void(), StartedLeading);
    DEFINE_SIGNAL(void(), StoppedLeading);

private:
    TBootstrap* const Bootstrap_;
    const TYTConnectorConfigPtr Config_;

    const TPeriodicExecutorPtr MasterDiscoveryExecutor_;

    TYPath DBPath_;

    INativeConnectionPtr Connection_;
    INativeClientPtr Client_;

    bool UdfsLoaded_ = false;
    TTypeInferrerMapPtr TypeInferrers_;
    TFunctionProfilerMapPtr FunctionProfilers_;

    ITransactionPtr InstanceLockTransaction_;
    ITransactionPtr LeaderLockTransaction_;

    bool IsConnectScheduled_ = false;
    std::atomic<bool> IsConnected_ = {false};

    bool IsTakeLeaderLockScheduled_ = false;
    std::atomic<bool> IsLeading_ = {false};

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;

    TReaderWriterSpinLock MasterDiscoveryLock_;
    std::vector<TMasterDiscoveryInfo> MasterDiscoveryInfos_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    const NLogging::TLogger Logger = NMaster::Logger;


    void ScheduleConnect(bool immediately = false)
    {
        if (IsConnectScheduled_) {
            return;
        }
        
        IsConnectScheduled_ = true;
        TDelayedExecutor::Submit(
            BIND(&TImpl::Connect, MakeWeak(this))
                .Via(Bootstrap_->GetControlInvoker()),
            immediately ? TDuration::Zero() : Config_->ReconnectPeriod);
    }

    void Connect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        IsConnectScheduled_ = false;
        try {
            GuardedConnect();
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Error connecting to YT");
            Disconnect();
        }
    }

    void GuardedConnect()
    {
        LOG_INFO("Connecting to YT");

        const auto& fqdn = Bootstrap_->GetFqdn();
        auto instancePath = GetInstancesCypressPath() + "/" + ToYPathLiteral(fqdn);

        {
            LOG_INFO("Creating instance node");

            TCreateNodeOptions options;
            options.Recursive = true;
            options.Force = true;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("instance_tag", GetInstanceTag());
            attributes->Set("client_grpc_address", Bootstrap_->GetClientGrpcAddress());
            attributes->Set("client_http_address", Bootstrap_->GetClientHttpAddress());
            attributes->Set("agent_grpc_address", Bootstrap_->GetAgentGrpcAddress());
            options.Attributes = std::move(attributes);
            WaitFor(Client_->CreateNode(instancePath, EObjectType::MapNode, options))
                .ThrowOnError();

            LOG_INFO("Instance node created");
        }

        {
            LOG_INFO("Starting instance lock transaction");

            TTransactionStartOptions options;
            options.Timeout = Config_->InstanceTransactionTimeout;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Format("Instance lock for %v", fqdn));
            options.Attributes = std::move(attributes);
            InstanceLockTransaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master, options))
                .ValueOrThrow();

            LOG_INFO("Instance lock transaction started (TransactionId: %v)",
                InstanceLockTransaction_->GetId());
        }

        {
            LOG_INFO("Taking instance lock");

            WaitFor(InstanceLockTransaction_->LockNode(instancePath, ELockMode::Exclusive))
                .ThrowOnError();
        }

        if (!UdfsLoaded_) {
            std::vector<TString> functionNames;
            {
                LOG_INFO("Listing UDFs");
                auto result = WaitFor(Client_->ListNode(GetUdfsPath()))
                    .ValueOrThrow();
                functionNames = ConvertTo<std::vector<TString>>(result);
            }

            {
                LOG_INFO("Downloading UDFs (FunctionNames: %v)",
                    functionNames);

                auto externalCGInfo = New<TExternalCGInfo>();

                TypeInferrers_ = New<TTypeInferrerMap>();
                FunctionProfilers_ = New<TFunctionProfilerMap>();

                MergeFrom(TypeInferrers_.Get(), *BuiltinTypeInferrersMap);
                MergeFrom(FunctionProfilers_.Get(), *BuiltinFunctionProfilers);

                const auto& functionRegistry = Client_->GetFunctionRegistry();
                auto descriptors = WaitFor(functionRegistry->FetchFunctions(functionNames))
                    .ValueOrThrow();

                AppendUdfDescriptors(
                    TypeInferrers_,
                    externalCGInfo,
                    functionNames,
                    descriptors);

                FetchFunctionImplementationsFromCypress(
                    FunctionProfilers_,
                    nullptr,
                    externalCGInfo,
                    Client_->GetFunctionImplCache());
            }

            UdfsLoaded_ = true;
        }

        InstanceLockTransaction_->SubscribeAborted(
            BIND(
                &TImpl::OnInstanceTransactionAborted,
                MakeWeak(this),
                InstanceLockTransaction_->GetId())
            .Via(Bootstrap_->GetControlInvoker()));

        MasterDiscoveryExecutor_->ScheduleOutOfBand();

        ValidateConnection_.Fire();

        LOG_INFO("YT connected");

        YCHECK(!IsConnected());
        IsConnected_.store(true);
        CancelableContext_ = New<TCancelableContext>();
        CancelableInvoker_ = CancelableContext_->CreateInvoker(Bootstrap_->GetControlInvoker());

        Connected_.Fire();

        ScheduleTakeLeaderLock(true);
    }

    void Disconnect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        StopLeading();

        if (IsConnected()) {
            LOG_INFO("YT disconnected");
            Disconnected_.Fire();
        }

        InstanceLockTransaction_.Reset();
        if (CancelableContext_) {
            CancelableContext_->Cancel();
            CancelableContext_.Reset();
        }
        CancelableInvoker_.Reset();
        IsConnected_.store(false);
        IsLeading_.store(false);
        IsTakeLeaderLockScheduled_ = false;

        ScheduleConnect();
    }

    void OnInstanceTransactionAborted(const TTransactionId& id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!InstanceLockTransaction_ || InstanceLockTransaction_->GetId() != id) {
            return;
        }

        LOG_INFO("Instance lock transaction aborted; disconnecting");
        Disconnect();
    }


    void ScheduleTakeLeaderLock(bool immediately = false)
    {
        if (IsTakeLeaderLockScheduled_) {
            return;
        }
        IsTakeLeaderLockScheduled_ = true;
        TDelayedExecutor::Submit(
            BIND(&TImpl::TakeLeaderLock, MakeWeak(this))
                .Via(CancelableInvoker_),
            immediately ? TDuration::Zero() : Config_->ReconnectPeriod);
    }

    void TakeLeaderLock()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        IsTakeLeaderLockScheduled_ = false;
        try {
            GuardedTakeLeaderLock();
        } catch (const std::exception& ex) {
            LOG_INFO(ex, "Failed to take leader lock");
            ScheduleTakeLeaderLock();
        }
    }

    void GuardedTakeLeaderLock()
    {
        LOG_INFO("Trying to take leader lock");

        const auto& fqdn = Bootstrap_->GetFqdn();

        {
            LOG_INFO("Starting leader lock transaction");

            TTransactionStartOptions options;
            options.Timeout = Config_->LeaderTransactionTimeout;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Format("Leader lock for %v", fqdn));
            attributes->Set("fqdn", fqdn);
            options.Attributes = std::move(attributes);
            LeaderLockTransaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master, options))
                .ValueOrThrow();

            LOG_INFO("Leader lock transaction started (TransactionId: %v)",
                LeaderLockTransaction_->GetId());
        }

        {
            LOG_INFO("Taking leader lock");

            auto path = GetLeaderCypressPath();
            WaitFor(LeaderLockTransaction_->LockNode(path, ELockMode::Exclusive))
                .ThrowOnError();

            LOG_INFO("Leader lock taken");
        }

        LeaderLockTransaction_->SubscribeAborted(
            BIND(
                &TImpl::OnLeaderTransactionAborted,
                MakeWeak(this),
                LeaderLockTransaction_->GetId())
            .Via(Bootstrap_->GetControlInvoker()));

        LOG_INFO("Started leading");

        YCHECK(!IsLeading());
        IsLeading_.store(true);
        StartedLeading_.Fire();
    }

    void StopLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LeaderLockTransaction_.Reset();

        if (!IsLeading()) {
            return;
        }

        LOG_INFO("Stopped leading");

        IsLeading_.store(false);
        StoppedLeading_.Fire();
    }

    void OnLeaderTransactionAborted(const TTransactionId& id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!LeaderLockTransaction_ || LeaderLockTransaction_->GetId() != id) {
            return;
        }

        LOG_INFO("Leader lock transaction aborted");

        StopLeading();
        ScheduleTakeLeaderLock();
    }


    void OnMasterDiscovery()
    {
        LOG_INFO("Master discovery started");

        try {
            TNullable<TString> leaderFqdn;
            {
                auto path = GetLeaderCypressPath() + "/@locks";
                auto yson = WaitFor(Client_->GetNode(path))
                    .ValueOrThrow();

                auto locks = ConvertTo<IListNodePtr>(yson);
                if (locks->GetChildCount() > 1) {
                    THROW_ERROR_EXCEPTION("More than one leader lock found");
                }

                if (locks->GetChildCount() == 1) {
                    auto transactionId = ConvertTo<TTransactionId>(locks->GetChild(0)->AsMap()->GetChild("transaction_id"));
                    auto leaderFqdnYson = WaitFor(Client_->GetNode(Format("%v%v/@fqdn", ObjectIdPathPrefix, transactionId)))
                        .ValueOrThrow();
                    leaderFqdn = ConvertTo<TString>(leaderFqdnYson);
                    LOG_DEBUG("Leader discovered (Fqdn: %v)",
                        leaderFqdn);
                }
            }

            TYsonString instancesYson;
            {
                auto path = GetInstancesCypressPath();
                TListNodeOptions options;
                options.Attributes = std::vector<TString>{
                    "instance_tag",
                    "client_grpc_address",
                    "client_http_address",
                    "agent_grpc_address",
                    "lock_count"
                };
                instancesYson = WaitFor(Client_->ListNode(path, options))
                    .ValueOrThrow();
            }

            std::vector<TMasterDiscoveryInfo> masterDiscoveryInfos;
            auto instancesList = ConvertTo<IListNodePtr>(instancesYson);
            for (const auto& child : instancesList->GetChildren()) {
                TMasterDiscoveryInfo info;
                info.Fqdn = child->GetValue<TString>();
                info.ClientGrpcAddress = child->Attributes().Get<TString>("client_grpc_address");
                info.ClientHttpAddress = child->Attributes().Get<TString>("client_http_address");
                info.AgentGrpcAddress = child->Attributes().Get<TString>("agent_grpc_address");
                info.InstanceTag = child->Attributes().Get<TMasterInstanceTag>("instance_tag");
                info.Alive = child->Attributes().Get<int>("lock_count") > 0;
                info.Leading = (leaderFqdn && *leaderFqdn == info.Fqdn);
                masterDiscoveryInfos.push_back(info);

                LOG_DEBUG("Master discovered (Fqdn: %v, ClientGrpcAddress: %v, ClientHttpAddress: %v, AgentGrpcAddress: %v, "
                    "InstanceTag: %v, Alive: %v, Leading: %v)",
                    info.Fqdn,
                    info.ClientGrpcAddress,
                    info.ClientHttpAddress,
                    info.AgentGrpcAddress,
                    info.InstanceTag,
                    info.Alive,
                    info.Leading);
            }

            {
                TWriterGuard guard(MasterDiscoveryLock_);
                std::swap(MasterDiscoveryInfos_, masterDiscoveryInfos);
            }
            LOG_INFO("Master discovery completed");
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Master discovery failed");
        }
    }


    TYPath GetInstancesCypressPath()
    {
        return GetRootPath() + "/instances";
    }

    TYPath GetLeaderCypressPath()
    {
        return GetRootPath() + "/leader";
    }

    TYPath GetUdfsPath()
    {
        return GetRootPath() + "/udfs";
    }
};

////////////////////////////////////////////////////////////////////////////////

TYTConnector::TYTConnector(TBootstrap* bootstrap, TYTConnectorConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, std::move(config)))
{ }

void TYTConnector::Initialize()
{
    Impl_->Initialize();
}

const INativeClientPtr& TYTConnector::GetClient()
{
    return Impl_->GetClient();
}

const TTypeInferrerMapPtr& TYTConnector::GetTypeInferrers()
{
    return Impl_->GetTypeInferrers();
}

const TFunctionProfilerMapPtr& TYTConnector::GetFunctionProfilers()
{
    return Impl_->GetFunctionProfilers();
}

const TYPath& TYTConnector::GetRootPath()
{
    return Impl_->GetRootPath();
}

const TYPath& TYTConnector::GetDBPath()
{
    return Impl_->GetDBPath();
}

TYPath TYTConnector::GetTablePath(const NObjects::TDbTable* table)
{
    return Impl_->GetTablePath(table);
}

TClusterTag TYTConnector::GetClusterTag()
{
    return Impl_->GetClusterTag();
}

TMasterInstanceTag TYTConnector::GetInstanceTag()
{
    return Impl_->GetInstanceTag();
}

bool TYTConnector::IsConnected()
{
    return Impl_->IsConnected();
}

bool TYTConnector::IsLeading()
{
    return Impl_->IsLeading();
}

const ITransactionPtr& TYTConnector::GetInstanceLockTransaction()
{
    return Impl_->GetInstanceLockTransaction();
}

std::vector<TMasterDiscoveryInfo> TYTConnector::GetMasters()
{
    return Impl_->GetMasters();
}

DELEGATE_SIGNAL(TYTConnector, void(), Connected, *Impl_);
DELEGATE_SIGNAL(TYTConnector, void(), Disconnected, *Impl_);
DELEGATE_SIGNAL(TYTConnector, void(), ValidateConnection, *Impl_);

DELEGATE_SIGNAL(TYTConnector, void(), StartedLeading, *Impl_);
DELEGATE_SIGNAL(TYTConnector, void(), StoppedLeading, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NMaster
} // namespace NServer
} // namespace NYP

