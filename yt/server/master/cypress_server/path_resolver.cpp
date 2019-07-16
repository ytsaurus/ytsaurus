#include "path_resolver.h"
#include "cypress_manager.h"
#include "node_detail.h"
#include "portal_entrance_node.h"
#include "helpers.h"

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/object_server/object_manager.h>

#include <yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/ytree/node.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NTransactionServer;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TPathResolver::TPathResolver(
    TBootstrap* bootstrap,
    const TString& service,
    const TString& method,
    const NYPath::TYPath& path,
    TTransactionId transactionId)
    : Bootstrap_(bootstrap)
    , Service_(service)
    , Method_(method)
    , Path_(path)
    , TransactionId_(transactionId)
    , Tokenizer_(path)
{ }

TPathResolver::TResolveResult TPathResolver::Resolve()
{
    if (Service_ == TMasterYPathProxy::GetDescriptor().ServiceName) {
        return TResolveResult{
            Path_,
            TLocalObjectPayload{
                Bootstrap_->GetObjectManager()->GetMasterObject(),
                GetTransaction()
            }
        };
    }

    static const auto EmptyYPath = TYPath("");
    static const auto SlashYPath = TYPath("/");
    static const auto AmpersandYPath = TYPath("&");

    // Nullptr indicates that one must resolve the root.
    TObjectBase* currentObject = nullptr;

    for (int resolveDepth = 0; ; ++resolveDepth) {
        ValidateYPathResolutionDepth(Path_, resolveDepth);

        if (!currentObject) {
            auto rootPayload = ResolveRoot();
            if (!std::holds_alternative<TLocalObjectPayload>(rootPayload)) {
                return {
                    TYPath(Tokenizer_.GetInput()),
                    std::move(rootPayload)
                };
            }
            currentObject = std::get<TLocalObjectPayload>(rootPayload).Object;
        }

        bool ampersandSkipped = Tokenizer_.Skip(NYPath::ETokenType::Ampersand);
        bool slashSkipped = Tokenizer_.Skip(NYPath::ETokenType::Slash);
        auto makeCurrentUnresolvedPath = [&] {
            return
                (ampersandSkipped ? AmpersandYPath : EmptyYPath) +
                (slashSkipped ? SlashYPath : EmptyYPath) +
                Tokenizer_.GetInput();
        };
        auto makeCurrentLocalObjectResult = [&] {
            auto* trunkObject = currentObject->IsTrunk() ? currentObject : currentObject->As<TCypressNodeBase>()->GetTrunkNode();
            auto unresolvedPath = makeCurrentUnresolvedPath();
            return TResolveResult{
                std::move(unresolvedPath),
                TLocalObjectPayload{
                    trunkObject,
                    GetTransaction()
                }
            };
        };

        if (!IsVersionedType(currentObject->GetType())) {
            return makeCurrentLocalObjectResult();
        }

        auto* currentNode = currentObject->As<TCypressNodeBase>();
        if (currentNode->GetNodeType() == ENodeType::Map || currentNode->GetNodeType() == ENodeType::List) {
            if (!slashSkipped) {
                return makeCurrentLocalObjectResult();
            }

            if (Tokenizer_.GetType() != NYPath::ETokenType::Literal) {
                return makeCurrentLocalObjectResult();
            }

            auto token = Tokenizer_.GetToken();

            if (currentNode->GetNodeType() == ENodeType::List &&
                IsSpecialListKey(token))
            {
                return makeCurrentLocalObjectResult();
            }

            auto* child = currentNode->GetNodeType() == ENodeType::Map
                ? FindMapNodeChild(currentNode, GetTransaction(), token)
                : FindListNodeChild(currentNode, token);

            if (!IsObjectAlive(child)) {
                return makeCurrentLocalObjectResult();
            }

            Tokenizer_.Advance();

            currentObject = child;
        } else if (currentNode->GetType() == EObjectType::Link) {
            if (ampersandSkipped) {
                return makeCurrentLocalObjectResult();
            }

            if (!slashSkipped) {
                if (Tokenizer_.GetType() != NYPath::ETokenType::EndOfStream ||
                    (Method_ == "Remove" || Method_ == "Create" || Method_ == "Copy"))
                {
                    return makeCurrentLocalObjectResult();
                }
            }

            const auto* link = currentNode->As<TLinkNode>();
            auto rewrittenPath =
                link->GetTargetPath() +
                (slashSkipped ? SlashYPath : EmptyYPath) +
                Tokenizer_.GetInput();
            Tokenizer_.Reset(std::move(rewrittenPath));

            ++resolveDepth;

            // Reset currentObject to request root resolve at the beginning of the next iteration.
            currentObject = nullptr;
        } else if (currentNode->GetType() == EObjectType::PortalEntrance) {
            if (ampersandSkipped) {
                return makeCurrentLocalObjectResult();
            }

            const auto* portalEntrance = currentNode->As<TPortalEntranceNode>();
            auto portalExitNodeId = MakePortalExitNodeId(
                portalEntrance->GetId(),
                portalEntrance->GetExitCellTag());

            return TResolveResult{
                makeCurrentUnresolvedPath(),
                TRemoteObjectPayload{portalExitNodeId}
            };
        } else  {
            return makeCurrentLocalObjectResult();
        }
    }
}

TTransaction* TPathResolver::GetTransaction()
{
    if (!Transaction_) {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        Transaction_ = TransactionId_
            ? transactionManager->GetTransactionOrThrow(TransactionId_)
            : nullptr;
    }
    return *Transaction_;
}

TPathResolver::TResolvePayload TPathResolver::ResolveRoot()
{
    switch (Tokenizer_.Advance()) {
        case ETokenType::EndOfStream:
            THROW_ERROR_EXCEPTION("YPath cannot be empty");

        case ETokenType::Slash: {
            Tokenizer_.Advance();
            return TLocalObjectPayload{
                Bootstrap_->GetCypressManager()->GetRootNode(),
                GetTransaction()
            };
        }

        case ETokenType::Literal: {
            const auto& token = Tokenizer_.GetToken();
            if (!token.StartsWith(ObjectIdPathPrefix)) {
                Tokenizer_.ThrowUnexpected();
            }

            TStringBuf objectIdString(token.begin() + ObjectIdPathPrefix.length(), token.end());
            TObjectId objectId;
            if (!TObjectId::FromString(objectIdString, &objectId)) {
                THROW_ERROR_EXCEPTION("Error parsing object id %v",
                    objectIdString);
            }
            Tokenizer_.Advance();

            if (CellTagFromId(objectId) != Bootstrap_->GetCellTag() && Bootstrap_->IsPrimaryMaster()) {
                return TRemoteObjectPayload{objectId};
            }

            const auto& objectManager = Bootstrap_->GetObjectManager();
            auto* root = Method_ == "Exists"
                ? objectManager->FindObject(objectId)
                : objectManager->GetObjectOrThrow(objectId);
            if (IsObjectAlive(root)) {
                return TLocalObjectPayload{
                    root,
                    GetTransaction()
                };
            } else {
                return TMissingObjectPayload{};
            }
        }

        default:
            Tokenizer_.ThrowUnexpected();
            YT_ABORT();
    }
}

TObjectBase* TPathResolver::FindMapNodeChild(TObjectBase* map, TTransaction* transaction, TStringBuf key)
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    return NCypressServer::FindMapNodeChild(cypressManager, map->As<TMapNode>()->GetTrunkNode(), transaction, key);
}

TObjectBase* TPathResolver::FindListNodeChild(TObjectBase* list, TStringBuf key)
{
    auto& indexToChild = list->As<TListNode>()->IndexToChild();
    auto indexStr = ExtractListIndex(key);
    int index = ParseListIndex(indexStr);
    auto adjustedIndex = TryAdjustChildIndex(index, static_cast<int>(indexToChild.size()));
    if (!adjustedIndex) {
        return nullptr;
    }
    return indexToChild[*adjustedIndex];

}

bool TPathResolver::IsSpecialListKey(TStringBuf key)
{
    return
        key == ListBeginToken ||
        key == ListEndToken ||
        key.StartsWith(ListBeforeToken) ||
        key.StartsWith(ListAfterToken);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
