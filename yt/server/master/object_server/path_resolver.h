#pragma once

#include "public.h"

#include <yt/server/master/object_server/public.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/server/master/cell_master/public.h>

#include <yt/core/ypath/tokenizer.h>

#include <variant>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TPathResolver
{
public:
    TPathResolver(
        NCellMaster::TBootstrap* bootstrap,
        const TString& service,
        const TString& method,
        const NYPath::TYPath& path,
        NTransactionClient::TTransactionId transactionId);

    struct TLocalObjectPayload
    {
        TObjectBase* Object;
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
    };

    TResolveResult Resolve();

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
    TPathResolver::TResolvePayload ResolveRoot();
    TObjectBase* FindMapNodeChild(
        TObjectBase* map,
        NTransactionServer::TTransaction* transaction,
        TStringBuf key);
    TObjectBase* FindListNodeChild(
        TObjectBase* list,
        TStringBuf key);
    static bool IsSpecialListKey(TStringBuf key);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
