#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <variant>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct TPathResolverOptions
{
    bool EnablePartialResolve = true;
    bool PopulateResolveCache = false;
    std::optional<int> SymlinkEncounterCountLimit;
    int InitialResolveDepth = 0;
    bool AllowResolveFromSequoiaObject = false;
};

class TPathResolver
{
public:
    using TTransactionToken = std::variant<
        NTransactionClient::TTransactionId,
        NTransactionServer::TTransaction*>;
    TPathResolver(
        NCellMaster::TBootstrap* bootstrap,
        std::string service,
        std::string method,
        const NYPath::TYPath& path,
        TTransactionToken transactionToken);

    struct TLocalObjectPayload
    {
        TObject* Object = nullptr;
        NTransactionServer::TTransaction* Transaction = nullptr;
    };

    struct TRemoteObjectPayload
    {
        NObjectClient::TObjectId ObjectId;
        int ResolveDepth = 0;
    };

    struct TSequoiaRedirectPayload
    {
        NCypressClient::TNodeId RootstockNodeId;
        NYTree::TYPath RootstockPath;
    };

    struct TMissingObjectPayload
    { };

    using TResolvePayload = std::variant<
        TLocalObjectPayload,
        TRemoteObjectPayload,
        TSequoiaRedirectPayload,
        TMissingObjectPayload
    >;

    struct TResolveResult
    {
        NYPath::TYPath UnresolvedPathSuffix;
        TResolvePayload Payload;
        bool CanCacheResolve;
        int ResolveDepth;
    };

    TResolveResult Resolve(const TPathResolverOptions& options = {});

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const std::string Service_;
    const std::string Method_;
    const NYPath::TYPath& Path_;
    const NTransactionClient::TTransactionId TransactionId_;

    NYPath::TTokenizer Tokenizer_;

    //! std::optional implements lazy semantics.
    //! Transaction is only needed when the actual resolve happens.
    //! However, if an immediate redirect happens then we should just pass it to the appropriate cell, as is.
    std::optional<NTransactionServer::TTransaction*> Transaction_;

    NTransactionServer::TTransaction* GetTransaction();
    TResolvePayload ResolveRoot(const TPathResolverOptions& options, bool treatCypressRootAsRemoteOnSecondaryMaster);

    // COMPAT(kvk1920): remove after 24.2.
    bool IsBackupMethod() noexcept;
    void MaybeApplyNativeTransactionExternalizationCompat(TObjectId objectId);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
