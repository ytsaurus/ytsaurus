#include "path_resolver.h"
#include "object_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/node_detail.h>
#include <yt/yt/server/master/cypress_server/link_node.h>
#include <yt/yt/server/master/cypress_server/portal_entrance_node.h>
#include <yt/yt/server/master/cypress_server/rootstock_node.h>
#include <yt/yt/server/master/cypress_server/helpers.h>
#include <yt/yt/server/master/cypress_server/resolve_cache.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NObjectServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NTransactionServer;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

TTransactionId GetTransactionIdFromToken(TPathResolver::TTransactionToken token)
{
   return Visit(token,
       [] (TTransactionId id) {
           return id;
       },
       [] (TTransaction* transaction) {
           return GetObjectId(transaction);
       });
}

std::optional<TTransaction*> GetTransactionFromToken(TPathResolver::TTransactionToken token)
{
   return Visit(token,
       [] (TTransactionId /*id*/) -> std::optional<TTransaction*> {
           return std::nullopt;
       },
       [] (TTransaction* transaction) -> std::optional<TTransaction*> {
           return transaction;
       });
}

} // namespace

TPathResolver::TPathResolver(
    TBootstrap* bootstrap,
    const TString& service,
    const TString& method,
    const NYPath::TYPath& path,
    TTransactionToken transactionToken)
    : Bootstrap_(bootstrap)
    , Service_(service)
    , Method_(method)
    , Path_(path)
    , TransactionId_(GetTransactionIdFromToken(transactionToken))
    , Transaction_(GetTransactionFromToken(transactionToken))
{ }

TPathResolver::TResolveResult TPathResolver::Resolve(const TPathResolverOptions& options)
{
    if (Service_ == TMasterYPathProxy::GetDescriptor().ServiceName) {
        YT_VERIFY(options.EnablePartialResolve);
        return TResolveResult{
            Path_,
            TLocalObjectPayload{
                Bootstrap_->GetObjectManager()->GetMasterObject(),
                GetTransaction()
            },
            /*canCacheResolve*/ false,
            /*resolveDepth*/ 0
        };
    }

    Tokenizer_.Reset(Path_);

    static const auto EmptyYPath = TYPath();
    static const auto SlashYPath = TYPath("/");
    static const auto AmpersandYPath = TYPath("&");

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    const auto& resolveCache = cypressManager->GetResolveCache();

    // Nullptr indicates that one must resolve the root.
    TObject* currentObject = nullptr;

    TResolveCacheNodePtr parentCacheNode;
    TString parentChildKey;

    bool canCacheResolve = true;
    int symlinksPassed = 0;
    TYPath rewrittenPath;

    for (int resolveDepth = options.InitialResolveDepth; ; ++resolveDepth) {
        ValidateYPathResolutionDepth(Path_, resolveDepth);

        if (!currentObject) {
            auto rootPayload = ResolveRoot();
            if (!std::holds_alternative<TLocalObjectPayload>(rootPayload)) {
                return {
                    TYPath(Tokenizer_.GetInput()),
                    std::move(rootPayload),
                    false,
                    resolveDepth
                };
            }
            currentObject = std::get<TLocalObjectPayload>(rootPayload).Object;
        }

        auto unresolvedPathSuffix = Tokenizer_.GetInput();
        bool ampersandSkipped = Tokenizer_.Skip(NYPath::ETokenType::Ampersand);
        bool endOfStream = Tokenizer_.GetType() == NYPath::ETokenType::EndOfStream;
        bool slashSkipped = Tokenizer_.Skip(NYPath::ETokenType::Slash);
        auto makeCurrentLocalObjectResult = [&] {
            auto* trunkObject = currentObject->IsTrunk() ? currentObject : currentObject->As<TCypressNode>()->GetTrunkNode();
            if (!options.EnablePartialResolve && !endOfStream) {
                THROW_ERROR_EXCEPTION("%v has unexpected suffix %v",
                    Path_,
                    unresolvedPathSuffix);
            }
            return TResolveResult{
                TYPath(unresolvedPathSuffix),
                TLocalObjectPayload{
                    trunkObject,
                    GetTransaction()
                },
                false,
                resolveDepth
            };
        };

        if (!IsVersionedType(currentObject->GetType())) {
            return makeCurrentLocalObjectResult();
        }

        auto* currentNode = currentObject->As<TCypressNode>();
        canCacheResolve &= currentNode->CanCacheResolve();

        TResolveCacheNodePtr currentCacheNode;
        if (options.PopulateResolveCache) {
            currentCacheNode = resolveCache->FindNode(currentNode->GetId());
            if (!currentCacheNode) {
                auto* currentTrunkNode = currentNode->GetTrunkNode();
                auto currentNodePath = cypressManager->GetNodePath(currentTrunkNode, nullptr);
                currentCacheNode = resolveCache->TryInsertNode(currentTrunkNode, currentNodePath);
            }
            if (parentCacheNode) {
                resolveCache->AddNodeChild(parentCacheNode, currentCacheNode, parentChildKey);
                parentCacheNode.Reset();
                parentChildKey.clear();
            }
        }

        if (currentNode->GetNodeType() == ENodeType::Map || currentNode->GetNodeType() == ENodeType::List) {
            if (!slashSkipped) {
                return makeCurrentLocalObjectResult();
            }

            if (Tokenizer_.GetType() != NYPath::ETokenType::Literal) {
                return makeCurrentLocalObjectResult();
            }

            const auto& key = Tokenizer_.GetLiteralValue();

            if (currentNode->GetNodeType() == ENodeType::List && IsSpecialListKey(key)) {
                if (!options.EnablePartialResolve) {
                    THROW_ERROR_EXCEPTION("Unexpected YPath token %Qv", key);
                }
                return makeCurrentLocalObjectResult();
            }

            TObject* childNode;
            if (options.EnablePartialResolve) {
                childNode = currentNode->GetNodeType() == ENodeType::Map
                    ? FindMapNodeChild(cypressManager, currentNode->As<TMapNode>(), GetTransaction(), key)
                    : FindListNodeChild(cypressManager, currentNode->As<TListNode>(), GetTransaction(), key);
            } else {
                childNode = currentNode->GetNodeType() == ENodeType::Map
                    ? GetMapNodeChildOrThrow(cypressManager, currentNode->As<TMapNode>(), GetTransaction(), key)
                    : GetListNodeChildOrThrow(cypressManager, currentNode->As<TListNode>(), GetTransaction(), key);
            }

            if (options.PopulateResolveCache && currentNode->GetNodeType() == ENodeType::Map) {
                parentCacheNode = currentCacheNode;
                parentChildKey = key;
            }

            if (!IsObjectAlive(childNode)) {
                return makeCurrentLocalObjectResult();
            }

            Tokenizer_.Advance();

            currentObject = childNode;
        } else if (currentNode->GetType() == EObjectType::Link) {
            ++symlinksPassed;

            if (ampersandSkipped) {
                return makeCurrentLocalObjectResult();
            }

            if (options.SymlinkEncounterCountLimit && *options.SymlinkEncounterCountLimit == symlinksPassed) {
                return makeCurrentLocalObjectResult();
            }

            if (!slashSkipped &&
                (Tokenizer_.GetType() != NYPath::ETokenType::EndOfStream ||
                 Method_ == "Remove" ||
                 Method_ == "Set" ||
                 Method_ == "Create" ||
                 Method_ == "Copy" ||
                 Method_ == "BeginCopy" ||
                 Method_ == "EndCopy"))
            {
                return makeCurrentLocalObjectResult();
            }

            const auto* link = currentNode->As<TLinkNode>();
            rewrittenPath =
                link->ComputeEffectiveTargetPath() +
                (slashSkipped ? SlashYPath : EmptyYPath) +
                Tokenizer_.GetInput();
            Tokenizer_.Reset(rewrittenPath);

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
                TYPath(unresolvedPathSuffix),
                TRemoteObjectPayload{portalExitNodeId},
                canCacheResolve,
                resolveDepth
            };
        } else if (currentNode->GetType() == EObjectType::Rootstock) {
            if (ampersandSkipped) {
                return makeCurrentLocalObjectResult();
            }

            auto* rootstockNode = currentNode->As<TRootstockNode>();
            auto rootstockNodePath = cypressManager->GetNodePath(
                rootstockNode->GetTrunkNode(),
                GetTransaction());
            return TResolveResult{
                TYPath(unresolvedPathSuffix),
                TSequoiaRedirectPayload{
                    .RootstockNodeId = rootstockNode->GetId(),
                    .RootstockPath = rootstockNodePath,
                },
                canCacheResolve,
                resolveDepth
            };
        } else {
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
    Tokenizer_.Advance();
    auto ampersandSkipped = Tokenizer_.Skip(NYPath::ETokenType::Ampersand);

    switch (Tokenizer_.GetType()) {
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
            auto token = Tokenizer_.GetToken();
            if (!token.StartsWith(ObjectIdPathPrefix)) {
                Tokenizer_.ThrowUnexpected();
            }

            TStringBuf objectIdString(token.begin() + ObjectIdPathPrefix.length(), token.end());
            TObjectId objectId;
            if (!TObjectId::FromString(objectIdString, &objectId)) {
                THROW_ERROR_EXCEPTION(
                    NYTree::EErrorCode::ResolveError,
                    "Error parsing object id %v",
                    objectIdString);
            }
            Tokenizer_.Advance();

            if (!objectId) {
                // Zero guid is often used to signify a null transaction and
                // should be treated as an ID of a special always-missing object.
                return TMissingObjectPayload{};
            }

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (CellTagFromId(objectId) != multicellManager->GetCellTag() &&
                multicellManager->IsPrimaryMaster() &&
                !ampersandSkipped &&
                !IsAlienType(TypeFromId(objectId)))
            {
                return TRemoteObjectPayload{objectId};
            }

            auto* transaction = GetTransaction();
            if (transaction && transaction->GetState(/*persistent*/ true) != ETransactionState::Active) {
                transaction->ThrowInvalidState();
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
