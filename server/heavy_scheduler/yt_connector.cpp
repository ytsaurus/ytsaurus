#include "yt_connector.h"

#include "bootstrap.h"
#include "config.h"
#include "private.h"

#include <yt/client/api/rpc_proxy/connection.h>

#include <yt/client/api/transaction.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_service.h>

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TYTConnector::TImpl
    : public TRefCounted
{
public:
    TImpl(TBootstrap* bootstrap, TYTConnectorConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , Connection_(NRpcProxy::CreateConnection(Config_->Connection))
        , Client_(Connection_->CreateClient(GetClientOptions()))
        , ConnectExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::Connect, MakeWeak(this)),
            Config_->ConnectPeriod))
    { }

    void Initialize()
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetControlInvoker(), ControlThread);

        ConnectExecutor_->Start();

        YT_LOG_INFO("YT connector initialized (RootPath: %v)",
            Config_->RootPath);
    }

    bool IsLeading() const
    {
        return Leading_.load();
    }

    NYTree::IYPathServicePtr CreateOrchidService()
    {
        auto orchidProducer = BIND(&TImpl::BuildOrchid, MakeStrong(this));
        return NYTree::IYPathService::FromProducer(std::move(orchidProducer));
    }

private:
    TBootstrap* const Bootstrap_;
    const TYTConnectorConfigPtr Config_;

    const IConnectionPtr Connection_;
    const IClientPtr Client_;

    const TPeriodicExecutorPtr ConnectExecutor_;

    ITransactionPtr LeaderLockTransaction_;
    std::atomic<bool> Leading_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    TClientOptions GetClientOptions() const
    {
        TClientOptions options;
        options.PinnedUser = Config_->User;
        if (Config_->Token) {
            options.Token = Config_->Token;
        }
        return options;
    }

    NYPath::TYPath GetLeaderLockPath() const
    {
        return Config_->RootPath + "/leader";
    }

    void StartLeading(ITransactionPtr transaction)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(!LeaderLockTransaction_);
        LeaderLockTransaction_ = std::move(transaction);
        Leading_.store(true);

        YT_LOG_DEBUG("Started leading");
    }

    void StopLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LeaderLockTransaction_ = nullptr;
        Leading_.store(false);

        YT_LOG_DEBUG("Stopped leading");
    }

    void OnLeaderTransactionAborted(const TTransactionId& id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(LeaderLockTransaction_ && LeaderLockTransaction_->GetId() == id);

        StopLeading();
    }

    void Connect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (LeaderLockTransaction_) {
            return;
        }

        {
            auto transaction = TryTakeLeaderLock(
                Client_,
                GetLeaderLockPath(),
                Config_->LeaderTransactionTimeout,
                Bootstrap_->GetFqdn());

            if (!transaction) {
                return;
            }

            StartLeading(std::move(transaction));
        }

        LeaderLockTransaction_->SubscribeAborted(
            BIND(
                &TImpl::OnLeaderTransactionAborted,
                MakeWeak(this),
                LeaderLockTransaction_->GetId())
            .Via(Bootstrap_->GetControlInvoker()));
    }

    static ITransactionPtr TryTakeLeaderLock(
        const IClientPtr& client,
        const NYPath::TYPath& lockPath,
        TDuration timeout,
        const TString& fqdn)
    {
        try {
            return TakeLeaderLock(
                client,
                lockPath,
                timeout,
                fqdn);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error taking leader lock");
        }
        return nullptr;
    }

    static ITransactionPtr TakeLeaderLock(
        const IClientPtr& client,
        const NYPath::TYPath& lockPath,
        TDuration timeout,
        const TString& fqdn)
    {
        YT_LOG_INFO("Trying to take leader lock");

        ITransactionPtr transaction;
        {
            YT_LOG_INFO("Starting leader lock transaction");

            TTransactionStartOptions options;
            options.Timeout = timeout;
            auto attributes = NYTree::CreateEphemeralAttributes();
            attributes->Set("title", Format("Leader lock for %v", fqdn));
            attributes->Set("fqdn", fqdn);
            options.Attributes = std::move(attributes);
            transaction = WaitFor(client->StartTransaction(ETransactionType::Master, options))
                .ValueOrThrow();

            YT_LOG_INFO("Leader lock transaction started (TransactionId: %v)",
                transaction->GetId());
        }

        {
            YT_LOG_INFO("Taking leader lock");

            WaitFor(transaction->LockNode(lockPath, NCypressClient::ELockMode::Exclusive))
                .ThrowOnError();

            YT_LOG_INFO("Leader lock taken");
        }

        return transaction;
    }

    void BuildOrchid(NYson::IYsonConsumer* consumer) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto isLeading = IsLeading();

        NYTree::BuildYsonFluently(consumer)
            .BeginMap()
                .Item("is_leading").Value(isLeading)
            .EndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

TYTConnector::TYTConnector(TBootstrap* bootstrap, TYTConnectorConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, std::move(config)))
{ }

TYTConnector::~TYTConnector()
{ }

void TYTConnector::Initialize()
{
    Impl_->Initialize();
}

bool TYTConnector::IsLeading() const
{
    return Impl_->IsLeading();
}

NYTree::IYPathServicePtr TYTConnector::CreateOrchidService()
{
    return Impl_->CreateOrchidService();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
