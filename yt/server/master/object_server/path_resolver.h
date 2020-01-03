#pragma once

#include "public.h"

#include <yt/server/master/object_server/public.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/server/master/cypress_server/public.h>

#include <yt/server/master/cell_master/public.h>

#include <yt/core/ypath/tokenizer.h>

#include <variant>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct TPathResolverOptions
{
    bool EnablePartialResolve = true;
    bool PopulateResolveCache = false;
    bool FollowPortals = true;
};

class TPathResolver
{
public:
    using TTransactionToken = std::variant<
        NTransactionClient::TTransactionId,
        NTransactionServer::TTransaction*>;
    TPathResolver(
        NCellMaster::TBootstrap* bootstrap,
        const TString& service,
        const TString& method,
        const NYPath::TYPath& path,
        TTransactionToken transactionToken);

    struct TLocalObjectPayload
    {
        TObject* Object;
        NTransactionServer::TTransaction* Transaction;
    };

    struct TRemoteObjectPayload
    {
        NObjectClient::TObjectId ObjectId;
    };

    struct TMissingObjectPayload
    { };

    using TResolvePayload = std::variant<
        TLocalObjectPayload,
        TRemoteObjectPayload,
        TMissingObjectPayload
    >;

    struct TResolveResult
    {
        NYPath::TYPath UnresolvedPathSuffix;
        TResolvePayload Payload;
        bool CanCacheResolve;
    };

    TResolveResult Resolve(const TPathResolverOptions& options = {});

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const TString& Service_;
    const TString& Method_;
    const NYPath::TYPath& Path_;
    const NTransactionClient::TTransactionId TransactionId_;

    NYPath::TTokenizer Tokenizer_;

    //! std::optional implements lazy semantics.
    //! Transaction is only needed when the actual resolve happens.
    //! However, if an immediate redirect happens then we should just pass it to the appropriate cell, as is.
    std::optional<NTransactionServer::TTransaction*> Transaction_;

    NTransactionServer::TTransaction* GetTransaction();
    TResolvePayload ResolveRoot();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
