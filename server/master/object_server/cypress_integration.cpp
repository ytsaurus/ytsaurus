#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/epoch_history_manager.h>

#include <yt/server/master/object_server/object_manager.h>

#include <yt/server/master/cypress_server/virtual.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/ypath/tokenizer.h>
#include <yt/core/ypath/token.h>

#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/yson/producer.h>

namespace NYT::NObjectServer  {

using namespace NCypressClient;
using namespace NCypressServer;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYPath;
using namespace NRpc;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

class TEstimatedCreationTimeMap
    : public TVirtualMapBase
{
public:
    TEstimatedCreationTimeMap(
        TBootstrap* bootstrap,
        NYTree::INodePtr owningNode)
        : TVirtualMapBase(std::move(owningNode))
        , Bootstrap_(bootstrap)
    { }

private:
    TBootstrap* const Bootstrap_;

    class TRemoteService
        : public IYPathService
    {
    public:
        TRemoteService(TBootstrap* bootstrap, TObjectId objectId)
            : Bootstrap_(bootstrap)
            , ObjectId_(objectId)
        { }

        virtual TResolveResult Resolve(const TYPath& path, const IServiceContextPtr& context) override
        {
            const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
            if (ypathExt.mutating()) {
                THROW_ERROR_EXCEPTION("Mutating requests to remote cells are not allowed");
            }

            YT_VERIFY(!HasMutationContext());

            return TResolveResultHere{path};
        }

        virtual void Invoke(const IServiceContextPtr& context) override
        {
            auto requestMessage = context->GetRequestMessage();
            auto requestHeader = context->RequestHeader();

            auto updatedYPath = GetWellKnownPath() + "/" + ToString(ObjectId_) + GetRequestTargetYPath(requestHeader);
            SetRequestTargetYPath(&requestHeader, updatedYPath);
            auto updatedMessage = SetRequestHeader(requestMessage, requestHeader);

            auto cellTag = CellTagFromId(ObjectId_);
            const auto& objectManager = Bootstrap_->GetObjectManager();
            auto asyncResponseMessage = objectManager->ForwardObjectRequest(updatedMessage, cellTag, EPeerKind::Follower);
            context->ReplyFrom(std::move(asyncResponseMessage));
        }

        virtual void DoWriteAttributesFragment(
            NYson::IAsyncYsonConsumer* /*consumer*/,
            const std::optional<std::vector<TString>>& /*attributeKeys*/,
            bool /*stable*/) override
        {
            YT_ABORT();
        }

        virtual bool ShouldHideAttributes() override
        {
            return false;
        }

    private:
        TBootstrap* const Bootstrap_;
        const TObjectId ObjectId_;
    };

    static TYPath GetWellKnownPath()
    {
        return "//sys/estimated_creation_time";
    }

    virtual std::vector<TString> GetKeys(i64 limit) const
    {
        return {};
    }

    virtual i64 GetSize() const
    {
        return std::numeric_limits<i64>::max();
    }

    virtual IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        TObjectId objectId;
        if (!TObjectId::FromString(key, &objectId)) {
            THROW_ERROR_EXCEPTION("Error parsing object id %v", key);
        }

        // Cf. TPathResolver::ResolveRoot.
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster() && CellTagFromId(objectId) != multicellManager->GetCellTag()) {
            return New<TRemoteService>(Bootstrap_, objectId);
        } else {
            const auto& epochHistoryManager = Bootstrap_->GetEpochHistoryManager();
            auto version = NHydra::TVersion::FromRevision(CounterFromId(objectId));
            auto timeSpan = epochHistoryManager->GetEstimatedMutationTime(version);
            return IYPathService::FromProducer(BIND([timeSpan] (NYson::IYsonConsumer* consumer) {
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("min").Value(timeSpan.first)
                        .Item("max").Value(timeSpan.second)
                    .EndMap();
            }));
        }
    }
};

INodeTypeHandlerPtr CreateEstimatedCreationTimeMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::EstimatedCreationTimeMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TEstimatedCreationTimeMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

}
