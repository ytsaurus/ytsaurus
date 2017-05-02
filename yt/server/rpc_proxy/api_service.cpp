#include "api_service.h"
#include "public.h"
#include "private.h"

#include <yt/server/cell_proxy/bootstrap.h>

#include <yt/server/blackbox/cookie_authenticator.h>
#include <yt/server/blackbox/token_authenticator.h>

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/transaction.h>
#include <yt/ytlib/api/rowset.h>

#include <yt/ytlib/rpc_proxy/api_service_proxy.h>
#include <yt/ytlib/rpc_proxy/helpers.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/row_buffer.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/serialize.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT {
namespace NRpcProxy {

using namespace NApi;
using namespace NYTree;
using namespace NConcurrency;
using namespace NRpc;
using namespace NCompression;
using namespace NBlackbox;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NObjectClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

struct TApiServiceBufferTag
{ };

class TApiService
    : public TServiceBase
{
public:
    TApiService(
        NCellProxy::TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(), // TODO(sandello): Better threading here.
            TApiServiceProxy::GetDescriptor(),
            RpcProxyLogger)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetNode));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupRows));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(VersionedLookupRows));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SelectRows));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ModifyRows));
    }

private:
    NCellProxy::TBootstrap* const Bootstrap_;

    TSpinLock SpinLock_;
    // TODO(sandello): Introduce expiration times for clients.
    yhash<Stroka, INativeClientPtr> AuthenticatedClients_;

    INativeClientPtr GetAuthenticatedClientOrAbortContext(const IServiceContextPtr& context)
    {
        auto replyWithMissingCredentials = [&] () {
            context->Reply(TError(
                NSecurityClient::EErrorCode::AuthenticationError,
                "Request is missing credentials"));
        };

        auto replyWithMissingUserIP = [&] () {
            context->Reply(TError(
                NSecurityClient::EErrorCode::AuthenticationError,
                "Request is missing originating address in credentials"));
        };

        const auto& header = context->GetRequestHeader();
        if (!header.HasExtension(NProto::TCredentialsExt::credentials_ext)) {
            replyWithMissingCredentials();
            return nullptr;
        }

        // TODO(sandello): Use a cache here.
        TAuthenticationResult authenticationResult;
        const auto& credentials = header.GetExtension(NProto::TCredentialsExt::credentials_ext);
        if (!credentials.has_userip()) {
            replyWithMissingUserIP();
            return nullptr;
        }
        if (credentials.has_sessionid() || credentials.has_sslsessionid()) {
            auto asyncAuthenticationResult = Bootstrap_->GetCookieAuthenticator()->Authenticate(
                credentials.sessionid(),
                credentials.sslsessionid(),
                credentials.domain(),
                credentials.userip());
            authenticationResult = WaitFor(asyncAuthenticationResult)
                .ValueOrThrow();
        } else if (credentials.has_token()) {
            auto asyncAuthenticationResult = Bootstrap_->GetTokenAuthenticator()->Authenticate(
                TTokenCredentials{credentials.token(), credentials.userip()});
            authenticationResult = WaitFor(asyncAuthenticationResult)
                .ValueOrThrow();
        } else {
            replyWithMissingCredentials();
            return nullptr;
        }

        const auto& user = context->GetUser();
        if (user != authenticationResult.Login) {
            context->Reply(TError(
                NSecurityClient::EErrorCode::AuthenticationError,
                "Invalid credentials"));
            return nullptr;
        }

        {
            auto guard = Guard(SpinLock_);
            auto it = AuthenticatedClients_.find(user);
            auto jt = AuthenticatedClients_.end();
            if (it == jt) {
                const auto& connection = Bootstrap_->GetNativeConnection();
                auto client = connection->CreateNativeClient(TClientOptions(user));
                bool inserted = false;
                std::tie(it, inserted) = AuthenticatedClients_.insert(std::make_pair(user, client));
                YCHECK(inserted);
            }
            return it->second;
        }
    }

    ITransactionPtr GetTransactionOrAbortContext(
        const IServiceContextPtr& context,
        const TTransactionId& transactionId,
        const TTransactionAttachOptions& options)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context);
        if (!client) {
            return nullptr;
        }

        auto transaction = client->AttachTransaction(transactionId, options);

        if (!transaction) {
            context->Reply(TError("No such transaction %v", transactionId));
            return nullptr;
        }

        context->SetRequestInfo("TransactionId: %v", transactionId);

        return transaction;
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, StartTransaction)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context);
        if (!client) {
            return;
        }

        TTransactionStartOptions options;
        if (request->has_timeout()) {
            options.Timeout = FromProto<TDuration>(request->timeout());
        }
        if (request->has_id()) {
            FromProto(&options.Id, request->id());
        }
        if (request->has_parent_id()) {
            FromProto(&options.ParentId, request->parent_id());
        }
        options.AutoAbort = request->auto_abort();
        options.Sticky = request->sticky();
        options.Ping = request->ping();
        options.PingAncestors = request->ping_ancestors();

        client->StartTransaction(NTransactionClient::ETransactionType(request->type()), options)
            .Subscribe(BIND([=] (const TErrorOr<ITransactionPtr>& result) {
                if (!result.IsOK()) {
                    context->Reply(result);
                } else {
                    const auto& value = result.Value();
                    ToProto(response->mutable_id(), value->GetId());
                    response->set_start_timestamp(value->GetStartTimestamp());
                    context->Reply();
                }
            }));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, PingTransaction)
    {
        auto transactionAttachOptions = TTransactionAttachOptions{};
        transactionAttachOptions.Ping = true;
        transactionAttachOptions.PingAncestors = true;
        transactionAttachOptions.Sticky = request->sticky();
        auto transaction = GetTransactionOrAbortContext(
            context,
            FromProto<TTransactionId>(request->transaction_id()),
            transactionAttachOptions);
        if (!transaction) {
            return;
        }

        // TODO(sandello): Options!
        transaction->Ping().Subscribe(BIND([=] (const TErrorOr<void>& result) {
            if (!result.IsOK()) {
                context->Reply(result);
            } else {
                context->Reply();
            }
        }));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, CommitTransaction)
    {
        auto transactionAttachOptions = TTransactionAttachOptions{};
        transactionAttachOptions.Ping = false;
        transactionAttachOptions.PingAncestors = false;
        transactionAttachOptions.Sticky = request->sticky();
        auto transaction = GetTransactionOrAbortContext(
            context,
            FromProto<TTransactionId>(request->transaction_id()),
            transactionAttachOptions);
        if (!transaction) {
            return;
        }

        // TODO(sandello): Options!
        transaction->Commit().Subscribe(BIND([=] (const TErrorOr<TTransactionCommitResult>& result) {
            if (!result.IsOK()) {
                context->Reply(result);
            } else {
                // TODO(sandello): Fill me.
                context->Reply();
            }
        }));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, AbortTransaction)
    {
        auto transactionAttachOptions = TTransactionAttachOptions{};
        transactionAttachOptions.Ping = false;
        transactionAttachOptions.PingAncestors = false;
        transactionAttachOptions.Sticky = request->sticky();
        auto transaction = GetTransactionOrAbortContext(
            context,
            FromProto<TTransactionId>(request->transaction_id()),
            transactionAttachOptions);
        if (!transaction) {
            return;
        }

        // TODO(sandello): Options!
        transaction->Abort().Subscribe(BIND([=] (const TErrorOr<void>& result) {
            if (!result.IsOK()) {
                context->Reply(result);
            } else {
                context->Reply();
            }
        }));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, GetNode)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context);
        if (!client) {
            return;
        }

        context->SetRequestInfo("Path: %v", request->path());

        // TODO(sandello): Inject options into req/rsp structure.
        auto options = TGetNodeOptions();
        client->GetNode(request->path(), options)
            .Subscribe(BIND([=] (const TErrorOr<NYson::TYsonString>& result) {
                if (!result.IsOK()) {
                    context->Reply(result);
                } else {
                    response->set_data(result.Value().GetData());
                    context->Reply();
                }
            }));
    }

    template <class TContext, class TRequest, class TOptions>
    static bool LookupRowsPrologue(
        const TIntrusivePtr<TContext>& context,
        TRequest* request,
        const NProto::TRowsetDescriptor& rowsetDescriptor,
        TNameTablePtr* nameTable,
        TSharedRange<TUnversionedRow>* keys,
        TOptions* options)
    {
        ValidateRowsetDescriptor(request->rowset_descriptor(), 1, NProto::ERowsetKind::UNVERSIONED);
        if (request->Attachments().empty()) {
            context->Reply(TError("Request is missing data"));
            return false;
        }

        auto rowset = DeserializeRowset<TUnversionedRow>(
            request->rowset_descriptor(),
            MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()));
        *nameTable = TNameTable::FromSchema(rowset->Schema());
        *keys = MakeSharedRange(rowset->GetRows(), rowset);

        options->Timeout = context->GetTimeout();
        for (int i = 0; i < request->columns_size(); ++i) {
            options->ColumnFilter.All = false;
            options->ColumnFilter.Indexes.push_back((*nameTable)->GetIdOrRegisterName(request->columns(i)));
        }
        options->Timestamp = request->timestamp();
        options->KeepMissingRows = request->keep_missing_rows();

        context->SetRequestInfo("Path: %v, Rows: %v", request->path(), keys->Size());

        return true;
    }

    template <class TContext, class TResponse, class TRow>
    static void LookupRowsEpilogue(
        const TIntrusivePtr<TContext>& context,
        TResponse* response,
        const TErrorOr<TIntrusivePtr<IRowset<TRow>>>& result)
    {
        if (!result.IsOK()) {
            context->Reply(result);
        } else {
            const auto& value = result.Value();
            response->Attachments() = SerializeRowset(
                value->Schema(),
                value->GetRows(),
                response->mutable_rowset_descriptor());
            context->Reply();
        }
    };

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, LookupRows)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context);
        if (!client) {
            return;
        }

        TNameTablePtr nameTable;
        TSharedRange<TUnversionedRow> keys;
        TLookupRowsOptions options;

        if (!LookupRowsPrologue(context, request, request->rowset_descriptor(), &nameTable, &keys, &options)) {
            return;
        }

        client->LookupRows(request->path(), std::move(nameTable), std::move(keys), options)
            .Subscribe(BIND([=] (const TErrorOr<IUnversionedRowsetPtr>& result) {
                LookupRowsEpilogue(context, response, result);
            }));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, VersionedLookupRows)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context);
        if (!client) {
            return;
        }

        TNameTablePtr nameTable;
        TSharedRange<TUnversionedRow> keys;
        TVersionedLookupRowsOptions options;

        if (!LookupRowsPrologue(context, request, request->rowset_descriptor(), &nameTable, &keys, &options)) {
            return;
        }

        client->VersionedLookupRows(request->path(), std::move(nameTable), std::move(keys), options)
            .Subscribe(BIND([=] (const TErrorOr<IVersionedRowsetPtr>& result) {
                LookupRowsEpilogue(context, response, result);
            }));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, SelectRows)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context);
        if (!client) {
            return;
        }

        TSelectRowsOptions options; // TODO: Fill all options.
        options.Timestamp = NTransactionClient::AsyncLastCommittedTimestamp;

        client->SelectRows(request->query(), options)
            .Subscribe(BIND([=] (const TErrorOr<TSelectRowsResult>& result) {
                if (!result.IsOK()) {
                    context->Reply(result);
                } else {
                    const auto& value = result.Value();
                    response->Attachments() = SerializeRowset(
                        value.Rowset->Schema(),
                        value.Rowset->GetRows(),
                        response->mutable_rowset_descriptor());
                    context->Reply();
                }
            }));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, ModifyRows)
    {
        auto transactionAttachOptions = TTransactionAttachOptions{};
        transactionAttachOptions.Ping = false;
        transactionAttachOptions.PingAncestors = false;
        transactionAttachOptions.Sticky = true; // XXX(sandello): Fix me!
        auto transaction = GetTransactionOrAbortContext(
            context,
            FromProto<TTransactionId>(request->transaction_id()),
            transactionAttachOptions);
        if (!transaction) {
            return;
        }

        auto rowset = DeserializeRowset<TUnversionedRow>(
            request->rowset_descriptor(),
            MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()));

        const auto& rowsetRows = rowset->GetRows();
        auto rowsetSize = rowset->GetRows().Size();

        if (rowsetSize != request->row_modification_types_size()) {
            THROW_ERROR_EXCEPTION("Row count mismatch");
        }

        std::vector<TRowModification> modifications;
        modifications.reserve(rowsetSize);
        for (size_t index = 0; index < rowsetSize; ++index) {
            modifications.push_back({
                ERowModificationType(request->row_modification_types(index)),
                rowsetRows[index]
            });
        }

        TModifyRowsOptions options;
        transaction->ModifyRows(
            request->path(),
            TNameTable::FromSchema(rowset->Schema()),
            MakeSharedRange(std::move(modifications), rowset),
            options);

        context->Reply();
    }

};

IServicePtr CreateApiService(
    NCellProxy::TBootstrap* bootstrap)
{
    return New<TApiService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT

