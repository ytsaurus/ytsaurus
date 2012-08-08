#include "stdafx.h"
#include "world_initializer.h"
#include "config.h"

#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/cypress_server/cypress_manager.h>
#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/object_server/object_service_proxy.h>
#include <ytlib/transaction_server/transaction_ypath_proxy.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/logging/log.h>

namespace NYT {
namespace NCellMaster {

using namespace NMetaState;
using namespace NYTree;
using namespace NCypressServer;
using namespace NCypressClient;
using namespace NTransactionServer;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

static TDuration CheckPeriod = TDuration::Seconds(1);
static NLog::TLogger Logger("Cypress");

////////////////////////////////////////////////////////////////////////////////

class TWorldInitializer::TImpl
    : public TRefCounted
{
public:
    TImpl(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    {
        YASSERT(bootstrap);

        PeriodicInvoker = New<TPeriodicInvoker>(
            bootstrap->GetStateInvoker(),
            BIND(&TImpl::OnCheck, MakeWeak(this)),
            CheckPeriod);
        PeriodicInvoker->Start();
    }

    bool IsInitialized() const
    {
        // 1 means just the root.
        // TODO(babenko): fixme
        return Bootstrap->GetCypressManager()->GetNodeCount() > 1;
    }

private:
    TBootstrap* Bootstrap;
    TPeriodicInvokerPtr PeriodicInvoker;

    void OnCheck()
    {
        if (IsInitialized()) {
            PeriodicInvoker->Stop();
        } else if (CanInitialize()) {
            Initialize();
            PeriodicInvoker->Stop();
        } else {
            PeriodicInvoker->ScheduleNext();
        }
    }

    bool CanInitialize() const
    {
        auto metaStateManager = Bootstrap->GetMetaStateManager();
        return
            metaStateManager->GetStateStatus() == EPeerStatus::Leading &&
            metaStateManager->HasActiveQuorum();
    }

    void Initialize()
    {
        LOG_INFO("World initialization started");

        try {
            auto objectManager = Bootstrap->GetObjectManager();
            auto cypressManager = Bootstrap->GetCypressManager();
            auto rootService = objectManager->GetRootService();

            auto cellId = objectManager->GetCellId();
            auto cellGuid = TGuid::Create();

            auto transactionId = StartTransaction();

            TYsonString emptyMap("{}");
            TYsonString opaqueEmptyMap("<opaque = true>{}");

            SyncYPathSet(
                rootService,
                WithTransaction("//sys", transactionId),
                emptyMap);

            SyncYPathSet(
                rootService,
                WithTransaction("//sys/@cell_id", transactionId),
                ConvertToYsonString(cellId));

            SyncYPathSet(
                rootService,
                WithTransaction("//sys/@cell_guid", transactionId),
                ConvertToYsonString(cellGuid));

            SyncYPathSet(
                rootService,
                WithTransaction("//sys/scheduler", transactionId),
                opaqueEmptyMap);

            SyncYPathSet(
                rootService,
                WithTransaction("//sys/scheduler/lock", transactionId),
                emptyMap);

            SyncYPathCreate(
                rootService,
                WithTransaction("//sys/scheduler/orchid", transactionId),
                EObjectType::Orchid);

            SyncYPathSet(
                rootService,
                WithTransaction("//sys/operations", transactionId),
                opaqueEmptyMap);

            SyncYPathCreate(
                rootService,
                WithTransaction("//sys/holders", transactionId),
                EObjectType::HolderMap);
            SyncYPathSet(
                rootService,
                WithTransaction("//sys/holders/@opaque", transactionId),
                TYsonString("true"));

            SyncYPathSet(
                rootService,
                WithTransaction("//sys/masters", transactionId),
                opaqueEmptyMap);

            FOREACH (const auto& address, Bootstrap->GetConfig()->MetaState->Cell->Addresses) {
                auto addressPath = "/" + EscapeYPathToken(address);
                SyncYPathSet(
                    rootService,
                    WithTransaction("//sys/masters" + addressPath, transactionId),
                    emptyMap);

                SyncYPathCreate(
                    rootService,
                    WithTransaction("//sys/masters" + addressPath + "/orchid", transactionId),
                    EObjectType::Orchid,
                    BuildYsonFluently()
                        .BeginMap()
                            .Item("remote_address").Scalar(address)
                        .EndMap().GetYsonString());
            }

            SyncYPathCreate(
                rootService,
                WithTransaction("//sys/chunks", transactionId),
                EObjectType::ChunkMap);

            SyncYPathCreate(
                rootService,
                WithTransaction("//sys/lost_chunks", transactionId),
                EObjectType::LostChunkMap);

            SyncYPathCreate(
                rootService,
                WithTransaction("//sys/overreplicated_chunks", transactionId),
                EObjectType::OverreplicatedChunkMap);

            SyncYPathCreate(
                rootService,
                WithTransaction("//sys/underreplicated_chunks", transactionId),
                EObjectType::UnderreplicatedChunkMap);

            SyncYPathCreate(
                rootService,
                WithTransaction("//sys/chunk_lists", transactionId),
                EObjectType::ChunkListMap);

            SyncYPathCreate(
                rootService,
                WithTransaction("//sys/nodes", transactionId),
                EObjectType::NodeMap);

            SyncYPathCreate(
                rootService,
                WithTransaction("//sys/transactions", transactionId),
                EObjectType::TransactionMap);

            SyncYPathSet(
                rootService,
                WithTransaction("//tmp", transactionId),
                emptyMap);

            SyncYPathSet(
                rootService,
                WithTransaction("//home", transactionId),
                emptyMap);

            CommitTransaction(transactionId);
        } catch (const std::exception& ex) {
            // TODO(babenko): this is wrong, we should retry!
            LOG_FATAL("World initialization failed\n%s", ex.what());
        }

        LOG_INFO("World initialization completed");
    }

    TTransactionId StartTransaction()
    {
        auto service = Bootstrap->GetObjectManager()->GetRootService();
        auto req = TTransactionYPathProxy::CreateObject(RootTransactionPath);
        req->set_type(EObjectType::Transaction);
        auto rsp = SyncExecuteVerb(service, req);
        return TTransactionId::FromProto(rsp->object_id());
    }

    void CommitTransaction(const TTransactionId& transactionId)
    {
        auto service = Bootstrap->GetObjectManager()->GetRootService();
        auto req = TTransactionYPathProxy::Commit(FromObjectId(transactionId));
        SyncExecuteVerb(service, req);
    }

    // TODO(babenko): consider moving somewhere
    static TObjectId SyncYPathCreate(
        IYPathServicePtr service,
        const TYPath& path,
        EObjectType type,
        const TYsonString& attributes = TYsonString("{}"))
    {
        auto req = TCypressYPathProxy::Create(path);
        req->set_type(type);
        req->Attributes().MergeFrom(ConvertToNode(attributes)->AsMap());
        auto rsp = SyncExecuteVerb(service, req);
        return TObjectId::FromProto(rsp->object_id());
    }
};

////////////////////////////////////////////////////////////////////////////////

TWorldInitializer::TWorldInitializer(TBootstrap* bootstrap)
    : Impl(New<TImpl>(bootstrap))
{ }

TWorldInitializer::~TWorldInitializer()
{ }

bool TWorldInitializer::IsInitialized() const
{
    return Impl->IsInitialized();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

