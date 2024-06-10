#include "object_service.h"

#include "bootstrap.h"
#include "config.h"
#include "dynamic_config_manager.h"
#include "helpers.h"
#include "private.h"
#include "sequoia_service_detail.h"

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/proto/object_ypath.pb.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient::NProto;
using namespace NObjectClient;
using namespace NRpc;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TObjectService
    : public IObjectService
    , public TServiceBase
{
public:
    TObjectService(IBootstrap* bootstrap)
        : TServiceBase(
            /*invoker*/ nullptr,
            TObjectServiceProxy::GetDescriptor(),
            CypressProxyLogger(),
            NullRealmId,
            bootstrap->GetNativeAuthenticator())
        , Bootstrap_(bootstrap)
        , Connection_(bootstrap->GetNativeConnection())
        , ThreadPool_(CreateThreadPool(/*threadCount*/ 1, "ObjectService"))
        , Invoker_(ThreadPool_->GetInvoker())
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetQueueSizeLimit(10'000)
            .SetConcurrencyLimit(10'000)
            .SetInvoker(Invoker_));

        DeclareServerFeature(EMasterFeature::Portals);
        DeclareServerFeature(EMasterFeature::PortalExitSynchronization);
    }

    void Reconfigure(const TObjectServiceDynamicConfigPtr& config) override
    {
        ThreadPool_->Configure(config->ThreadPoolSize);
    }

    IServicePtr GetService() override
    {
        return MakeStrong(this);
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, Execute);

    IBootstrap* const Bootstrap_;

    const NApi::NNative::IConnectionPtr Connection_;

    const IThreadPoolPtr ThreadPool_;
    const IInvokerPtr Invoker_;

    class TExecuteSession;
    using TExecuteSessionPtr = TIntrusivePtr<TExecuteSession>;
};

using TObjectServicePtr = TIntrusivePtr<TObjectService>;

////////////////////////////////////////////////////////////////////////////////

class TObjectService::TExecuteSession
    : public TRefCounted
{
public:
    TExecuteSession(
        TObjectServicePtr owner,
        TCtxExecutePtr rpcContext,
        TObjectServiceProxy masterProxy)
        : Owner_(std::move(owner))
        , RpcContext_(std::move(rpcContext))
        , MasterProxy_(std::move(masterProxy))
    { }

    void Run()
    {
        try {
            GuardedRun();
        } catch (const std::exception& ex) {
            Reply(TError(ex));
        }
    }

private:
    const TObjectServicePtr Owner_;
    const TCtxExecutePtr RpcContext_;

    TObjectServiceProxy MasterProxy_;

    struct TSubrequest
    {
        TSharedRefArray RequestMessage;

        // Whether Sequoia said that request should be handled by master.
        bool ForcedNonSequoia = false;
    };
    std::vector<TSubrequest> Subrequests_;

    void GuardedRun()
    {
        ParseSubrequests();
        PredictNonSequoia();
        InvokeSequoiaRequests();
        InvokeMasterRequests();
        Reply();
    }

    void ParseSubrequests()
    {
        const auto& request = RpcContext_->Request();
        const auto& attachments = RpcContext_->RequestAttachments();

        auto subrequestCount = request.part_counts_size();
        Subrequests_.resize(subrequestCount);
        int currentPartIndex = 0;
        for (int index = 0; index < subrequestCount; ++index) {
            auto& subrequest = Subrequests_[index];

            auto partCount = request.part_counts(index);
            TSharedRefArrayBuilder messageBuilder(partCount);
            for (int partIndex = 0; partIndex < partCount; ++partIndex) {
                messageBuilder.Add(attachments[currentPartIndex++]);
            }
            subrequest.RequestMessage = messageBuilder.Finish();
        }
    }

    bool PredictNonSequoiaForSubrequest(const TSubrequest& subrequest)
    {
        auto context = CreateSequoiaContext(subrequest.RequestMessage, /*transaction*/ nullptr);
        auto method = context->GetMethod();

        //! Such requests already contain the information about target cell inside the TReqExecute message.
        if (method == "Fetch" ||
            method == "BeginUpload" ||
            method == "GetUploadParams" ||
            method == "EndUpload")
        {
            return true;
        }

        return false;
    }

    void PredictNonSequoia()
    {
        for (auto& subrequest : Subrequests_) {
            subrequest.ForcedNonSequoia = PredictNonSequoiaForSubrequest(subrequest);
        }
    }

    void InvokeMasterRequests()
    {
        const auto& request = RpcContext_->Request();

        std::vector<int> subrequestIndices;
        for (int index = 0; index < std::ssize(Subrequests_); ++index) {
            if (Subrequests_[index].ForcedNonSequoia) {
                subrequestIndices.push_back(index);
            }
        }

        // Fast path.
        if (subrequestIndices.empty()) {
            return;
        }

        auto req = MasterProxy_.Execute();

        // Copy request.
        req->CopyFrom(request);

        // Copy authentication identity.
        SetAuthenticationIdentity(req, RpcContext_->GetAuthenticationIdentity());

        // Copy some header extensions.
        auto copyHeaderExtension = [&] (auto tag) {
            if (RpcContext_->RequestHeader().HasExtension(tag)) {
                const auto& ext = RpcContext_->RequestHeader().GetExtension(tag);
                req->Header().MutableExtension(tag)->CopyFrom(ext);
            }
        };
        copyHeaderExtension(NObjectClient::NProto::TMulticellSyncExt::multicell_sync_ext);

        // Fill request with non-Sequoia requests.
        req->clear_part_counts();

        for (auto index : subrequestIndices) {
            const auto& subrequest = Subrequests_[index];
            const auto& requestMessage = subrequest.RequestMessage;
            req->add_part_counts(requestMessage.size());
            req->Attachments().insert(
                req->Attachments().end(),
                requestMessage.Begin(),
                requestMessage.End());
        }

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        auto& response = RpcContext_->Response();

        int currentPartIndex = 0;
        for (const auto& subresponse : rsp->subresponses()) {
            auto partCount = subresponse.part_count();
            auto partsRange = TRange<TSharedRef>(
                rsp->Attachments().begin() + currentPartIndex,
                rsp->Attachments().begin() + currentPartIndex + partCount);
            currentPartIndex += partCount;

            auto* subresponseInfo = response.add_subresponses();
            auto index = subrequestIndices[subresponse.index()];
            subresponseInfo->set_index(index);
            subresponseInfo->set_part_count(partCount);
            response.Attachments().insert(
                response.Attachments().end(),
                partsRange.begin(),
                partsRange.end());
        }
    }

    void InvokeSequoiaRequests()
    {
        auto& response = RpcContext_->Response();
        auto client = Owner_->Bootstrap_->GetSequoiaClient();

        for (int index = 0; index < std::ssize(Subrequests_); ++index) {
            auto& subrequest = Subrequests_[index];
            if (subrequest.ForcedNonSequoia) {
                continue;
            }

            const auto& sequoiaService = Owner_->Bootstrap_->GetSequoiaService();
            auto rspFuture = ExecuteVerb(
                sequoiaService,
                &subrequest.RequestMessage,
                client,
                CypressProxyLogger());
            auto rsp = WaitFor(rspFuture)
                .ValueOrThrow();
            if (CheckSubresponseError(rsp, NObjectClient::EErrorCode::RequestInvolvesCypress)) {
                subrequest.ForcedNonSequoia = true;
                continue;
            }

            auto* subresponseInfo = response.add_subresponses();
            subresponseInfo->set_index(index);
            subresponseInfo->set_part_count(rsp.size());
            response.Attachments().insert(
                response.Attachments().end(),
                rsp.Begin(),
                rsp.End());
        }
    }

    void Reply(const TError& error = {})
    {
        RpcContext_->Reply(error);
    }

    bool CheckSubresponseError(const TSharedRefArray& message, TErrorCode errorCode)
    {
        try {
            auto subresponse = New<NYTree::TYPathResponse>();
            subresponse->Deserialize(message);
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            return static_cast<bool>(error.FindMatching(errorCode));
        }

        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TObjectService, Execute)
{
    if (!request->has_cell_tag()) {
        THROW_ERROR_EXCEPTION("Cell tag is not provided in request");
    }

    if (!request->has_peer_kind()) {
        THROW_ERROR_EXCEPTION("Peer kind is not provided in request");
    }

    auto cellTag = FromProto<TCellTag>(request->cell_tag());
    auto peerKind = FromProto<EMasterChannelKind>(request->peer_kind());

    context->SetRequestInfo("CellTag: %v, PeerKind: %v, RequestCount: %v",
        cellTag,
        peerKind,
        request->part_counts_size());

    if (peerKind != EMasterChannelKind::Leader &&
        peerKind != EMasterChannelKind::Follower)
    {
        THROW_ERROR_EXCEPTION("Expected %Qv or %Qv peer kind, got %Qv",
            EMasterChannelKind::Leader,
            EMasterChannelKind::Follower,
            peerKind);
    }

    auto masterChannel = Connection_->GetMasterChannelOrThrow(peerKind, cellTag);
    auto proxy = TObjectServiceProxy::FromDirectMasterChannel(std::move(masterChannel));

    auto session = New<TObjectService::TExecuteSession>(
        MakeStrong(this),
        context,
        proxy);
    session->Run();
}

////////////////////////////////////////////////////////////////////////////////

IObjectServicePtr CreateObjectService(IBootstrap* bootstrap)
{
    return New<TObjectService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
