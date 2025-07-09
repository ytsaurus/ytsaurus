#include "path_resolver.h"
#include "object_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/helpers.h>
#include <yt/yt/server/master/cypress_server/link_node.h>
#include <yt/yt/server/master/cypress_server/node_detail.h>
#include <yt/yt/server/master/cypress_server/portal_entrance_node.h>
#include <yt/yt/server/master/cypress_server/rootstock_node.h>
#include <yt/yt/server/master/cypress_server/resolve_cache.h>

#include <yt/yt/server/master/transaction_server/config.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NObjectServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NTransactionServer;
using namespace NYPath;
using namespace NYTree;

using NTransactionClient::MakeExternalizedTransactionId;
using NTransactionClient::OriginalFromExternalizedTransactionId;

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
    std::string service,
    std::string method,
    const NYPath::TYPath& path,
    TTransactionToken transactionToken)
    : Bootstrap_(bootstrap)
    , Service_(std::move(service))
    , Method_(std::move(method))
    , Path_(path)
    , TransactionId_(GetTransactionIdFromToken(transactionToken))
    , Transaction_(GetTransactionFromToken(transactionToken))
{ }

TPathResolver::TResolveResult TPathResolver::Resolve(const TPathResolverOptions& options)
{
    if (Service_ == TMasterYPathProxy::GetDescriptor().ServiceName) {
        YT_VERIFY(options.EnablePartialResolve);
        return TResolveResult{
            .UnresolvedPathSuffix = Path_,
            .Payload = TLocalObjectPayload{
                Bootstrap_->GetObjectManager()->GetMasterObject(),
                GetTransaction()
            },
            .CanCacheResolve = false,
            .ResolveDepth = 0,
        };
    }

    Tokenizer_.Reset(Path_);

    static const auto EmptyYPath = TYPath();
    static const auto SlashYPath = TYPath("/");
    static const auto AmpersandYPath = TYPath("&");

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    const auto& resolveCache = cypressManager->GetResolveCache();

    // Nullptr indicates that one must resolve the root.
    TObject* currentObject = nullptr;

    TResolveCacheNodePtr parentCacheNode;
    std::string parentChildKey;

    bool canCacheResolve = true;
    int symlinksPassed = 0;
    TYPath rewrittenPath;
    bool isLink = false;

    for (int resolveDepth = options.InitialResolveDepth; resolveDepth <= MaxYPathResolveIterations; ++resolveDepth) {
        if (!currentObject) {
            auto resolveOptions = options;
            resolveOptions.InitialResolveDepth = resolveDepth;

            auto rootPayload = ResolveRoot(resolveOptions, /*afterTraversingLink*/ isLink);

            if (!std::holds_alternative<TLocalObjectPayload>(rootPayload)) {
                return TResolveResult{
                    .UnresolvedPathSuffix = TYPath(Tokenizer_.GetInput()),
                    .Payload = std::move(rootPayload),
                    .CanCacheResolve = canCacheResolve && multicellManager->IsSecondaryMaster() && isLink,
                    .ResolveDepth = resolveDepth
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
                .UnresolvedPathSuffix = TYPath(unresolvedPathSuffix),
                .Payload = TLocalObjectPayload{
                    trunkObject,
                    GetTransaction()
                },
                .ResolveDepth = resolveDepth,
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
                currentCacheNode = resolveCache->TryInsertNode(currentTrunkNode, currentNodePath, cypressManager);
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
                    ? FindMapNodeChild(cypressManager, currentNode->As<TCypressMapNode>(), GetTransaction(), key)
                    : FindListNodeChild(cypressManager, currentNode->As<TListNode>(), GetTransaction(), key);
            } else {
                childNode = currentNode->GetNodeType() == ENodeType::Map
                    ? GetMapNodeChildOrThrow(cypressManager, currentNode->As<TCypressMapNode>(), GetTransaction(), key)
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
                 Method_ == "LockCopySource" ||
                 Method_ == "LockCopyDestination" ||
                 Method_ == "AssembleTreeCopy"))
            {
                return makeCurrentLocalObjectResult();
            }

            const auto* link = currentNode->As<TLinkNode>();
            rewrittenPath =
                cypressManager->ComputeEffectiveLinkNodeTargetPath(link) +
                (slashSkipped ? SlashYPath : EmptyYPath) +
                Tokenizer_.GetInput();
            Tokenizer_.Reset(rewrittenPath);

            ++resolveDepth;

            // Reset currentObject to request root resolve at the beginning of the next iteration.
            currentObject = nullptr;
            isLink = true;
        } else if (currentNode->GetType() == EObjectType::PortalEntrance) {
            if (ampersandSkipped) {
                return makeCurrentLocalObjectResult();
            }

            const auto* portalEntrance = currentNode->As<TPortalEntranceNode>();
            auto portalExitNodeId = MakePortalExitNodeId(
                portalEntrance->GetId(),
                portalEntrance->GetExitCellTag());

            return TResolveResult{
                .UnresolvedPathSuffix = TYPath(unresolvedPathSuffix),
                .Payload = TRemoteObjectRedirectPayload{.ObjectId = portalExitNodeId},
                .CanCacheResolve = canCacheResolve,
                .ResolveDepth = resolveDepth,
            };
        } else if (currentNode->GetType() == EObjectType::Rootstock) {
            if (ampersandSkipped) {
                return makeCurrentLocalObjectResult();
            }

            const auto* rootstock = currentNode->As<TRootstockNode>();
            auto rootstockPath = cypressManager->GetNodePath(currentNode, nullptr);

            return TResolveResult{
                .UnresolvedPathSuffix = TYPath(unresolvedPathSuffix),
                .Payload = TSequoiaRedirectPayload{
                    .RootstockNodeId = rootstock->GetId(),
                    .RootstockPath = std::move(rootstockPath),
                },
                .CanCacheResolve = canCacheResolve,
                .ResolveDepth = resolveDepth,
            };
        } else {
            return makeCurrentLocalObjectResult();
        }
    }

    ValidateYPathResolutionDepth(Path_, MaxYPathResolveIterations + 1);
    YT_UNREACHABLE();
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

bool TPathResolver::IsBackupMethod() noexcept
{
    static THashSet<TStringBuf> BackupMethods = {
        "StartBackup",
        "CheckBackup",
        "StartRestore",
        "FinishBackup",
        "FinishRestore",
    };

    return BackupMethods.contains(Method_);
}

TPathResolver::TResolvePayload TPathResolver::ResolveRoot(
    const TPathResolverOptions& options, bool afterTraversingLink)
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    const auto& cypressManager = Bootstrap_->GetCypressManager();

    Tokenizer_.Advance();
    auto ampersandSkipped = Tokenizer_.Skip(NYPath::ETokenType::Ampersand);

    switch (Tokenizer_.GetType()) {
        case ETokenType::EndOfStream:
            THROW_ERROR_EXCEPTION("YPath cannot be empty");

        case ETokenType::Slash: {
            Tokenizer_.Advance();

            if (multicellManager->IsSecondaryMaster() && afterTraversingLink) {
                return TRemoteObjectRedirectPayload{
                    .ObjectId = MakeWellKnownId(EObjectType::MapNode, multicellManager->GetPrimaryCellTag()),
                    .ResolveDepth = options.InitialResolveDepth,
                };
            }

            return TLocalObjectPayload{
                cypressManager->GetRootNode(),
                GetTransaction(),
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

            // Resolve from Sequoia object id is prohibited by default to
            // prevent node request processing in a non-Sequoia way. The
            // exceptions are:
            // - if request has already been processed by a Cypress Proxy and
            //   was forwarded to Master, hence the
            //   |AllowResolveFromSequoiaObject| flag is set;
            // - if the method should be handled by Master;
            // - if request is sent to _external_ cell of some node. For now,
            //   there is no a simple way to check if request was intentionally
            //   sent to external cell. Thus, we just allow resolve if object is
            //   not native for current cell.
            // - if non-Cypress transaction is used. In such cases Cypress proxy
            //   has no chances to handle it in Sequoia. Typical case is
            //   requesting extended file attributes from external cell during
            //   file upload. In such case we should check if current method is
            //   not mutating.
            if (IsSequoiaId(objectId) &&
                IsVersionedType(TypeFromId(objectId)) &&
                !options.AllowResolveFromSequoiaObject &&
                !NSequoiaClient::IsMethodHandledByMaster(Method_) && // TODO(kvk1920): drop IsMethodHandledByMaster().
                CellTagFromId(objectId) == Bootstrap_->GetCellTag() &&
                (!TransactionId_ || IsCypressTransactionType(TypeFromId(TransactionId_))))
            {
                return TSequoiaRedirectPayload{};
            }

            if (CellTagFromId(objectId) != multicellManager->GetCellTag() &&
                (multicellManager->IsPrimaryMaster() || (multicellManager->IsSecondaryMaster() && afterTraversingLink)) &&
                !ampersandSkipped &&
                !IsAlienType(TypeFromId(objectId)))
            {
                return TRemoteObjectRedirectPayload{.ObjectId = objectId};
            }

            MaybeApplyNativeTransactionExternalizationCompat(objectId);

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
                    GetTransaction(),
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

void TPathResolver::MaybeApplyNativeTransactionExternalizationCompat(TObjectId objectId)
{
    const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    const auto& objectManager = Bootstrap_->GetObjectManager();
    TObject* root = nullptr;

    // COMPAT(kvk1920): remove after 24.2.
    if (TransactionId_ &&
        IsBackupMethod() &&
        IsTableType(TypeFromId(objectId)) &&
        CellTagFromId(objectId) != Bootstrap_->GetCellTag() &&
        config->TransactionManager->EnableNonStrictExternalizedTransactionUsage &&
        (root = objectManager->FindObject(objectId)))
    {
        YT_VERIFY(root->IsForeign());

        TTransaction* replicatedTransaction = nullptr;
        TTransaction* externalizedTransaction = nullptr;

        auto specifiedTransactionType = TypeFromId(TransactionId_);
        if (IsExternalizedTransactionType(specifiedTransactionType)) {
            externalizedTransaction = transactionManager->FindTransaction(TransactionId_);
            replicatedTransaction = transactionManager->FindTransaction(
                OriginalFromExternalizedTransactionId(TransactionId_));
        } else if (IsCypressTransactionType(specifiedTransactionType)) {
            replicatedTransaction = transactionManager->FindTransaction(TransactionId_);
            externalizedTransaction = transactionManager->FindTransaction(
                MakeExternalizedTransactionId(TransactionId_, root->GetNativeCellTag()));
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        if (externalizedTransaction &&
            cypressManager->FindNode({objectId, externalizedTransaction->GetId()}))
        {
            Transaction_ = externalizedTransaction;
        }
        if (replicatedTransaction &&
            cypressManager->FindNode({objectId, replicatedTransaction->GetId()}))
        {
            Transaction_ = replicatedTransaction;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TPathResolver::TResolvePayload& payload,
    TStringBuf /*spec*/)
{
    Visit(payload,
        [&] (const TPathResolver::TLocalObjectPayload& typedPayload) {
            builder->AppendFormat("{ObjectId: %v, TransactionId: %v}",
                GetObjectId(typedPayload.Object),
                GetObjectId(typedPayload.Transaction));
        },
        [&] (const TPathResolver::TRemoteObjectRedirectPayload& typedPayload) {
            builder->AppendFormat("{ObjectId: %v, ResolveDepth: %v}",
                typedPayload.ObjectId,
                typedPayload.ResolveDepth);
        },
        [&] (const TPathResolver::TSequoiaRedirectPayload& typedPayload) {
            builder->AppendFormat("{RootstockNodeId: %v, RootstockPath: %v}",
                typedPayload.RootstockNodeId,
                typedPayload.RootstockPath);
        },
        [&] (const TPathResolver::TMissingObjectPayload& /*typedPayload*/) {
            builder->AppendFormat("{}");
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
