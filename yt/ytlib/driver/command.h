#pragma once

#include "private.h"
#include "public.h"
#include "driver.h"
#include "query_callbacks_provider.h"

#include <core/misc/error.h>
#include <core/misc/mpl.h>

#include <core/ytree/public.h>
#include <core/ytree/yson_serializable.h>
#include <core/ytree/convert.h>

#include <core/yson/consumer.h>
#include <core/yson/parser.h>
#include <core/yson/writer.h>

#include <core/rpc/public.h>
#include <core/rpc/channel.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/hydra/rpc_helpers.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/security_client/public.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/scheduler/scheduler_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/api/connection.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TRequest
    : public virtual TYsonSerializable
{
    TRequest()
    {
        SetKeepOptions(true);
    }
};

typedef TIntrusivePtr<TRequest> TRequestPtr;

////////////////////////////////////////////////////////////////////////////////

struct TTransactionalRequest
    : public virtual TRequest
{
    NObjectClient::TTransactionId TransactionId;
    bool PingAncestors;

    TTransactionalRequest()
    {
        RegisterParameter("transaction_id", TransactionId)
            .Default(NObjectClient::NullTransactionId);
        RegisterParameter("ping_ancestor_transactions", PingAncestors)
            .Default(false);
    }
};

typedef TIntrusivePtr<TTransactionalRequest> TTransactionalRequestPtr;

////////////////////////////////////////////////////////////////////////////////

struct TMutatingRequest
    : public virtual TRequest
{
    NHydra::TMutationId MutationId;

    TMutatingRequest()
    {
        RegisterParameter("mutation_id", MutationId)
            .Default(NHydra::NullMutationId);
    }
};

typedef TIntrusivePtr<TMutatingRequest> TMutatingRequestPtr;

////////////////////////////////////////////////////////////////////////////////

struct TSuppressableAccessTrackingRequest
    : public virtual TRequest
{
    bool SuppressAccessTracking;

    TSuppressableAccessTrackingRequest()
    {
        RegisterParameter("suppress_access_tracking", SuppressAccessTracking)
            .Default(false);
    }
};

typedef TIntrusivePtr<TSuppressableAccessTrackingRequest> TAccessTrackingSuppressableRequestPtr;

////////////////////////////////////////////////////////////////////////////////

struct ICommandContext
    : public NApi::IConnection
{
    virtual TDriverConfigPtr GetConfig() = 0;
    virtual TQueryCallbacksProviderPtr GetQueryCallbacksProvider() = 0;

    virtual const TDriverRequest& Request() const = 0;

    virtual const TDriverResponse& Response() const = 0;
    virtual TDriverResponse& Response() = 0;

    virtual const NFormats::TFormat& GetInputFormat() = 0;
    virtual const NFormats::TFormat& GetOutputFormat() = 0;

    virtual NYTree::TYsonProducer CreateInputProducer() = 0;
    virtual std::unique_ptr<NYson::IYsonConsumer> CreateOutputConsumer() = 0;
};

///////////////////////////////////////////////////////////////////////////////

struct ICommand
    : public TRefCounted
{
    virtual void Execute(ICommandContextPtr context) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TCommandBase
    : public ICommand
{
protected:
    ICommandContextPtr Context;
    bool Replied;

    NApi::IClientPtr Client;
    std::unique_ptr<NObjectClient::TObjectServiceProxy> ObjectProxy;
    std::unique_ptr<NScheduler::TSchedulerServiceProxy> SchedulerProxy;

    TCommandBase();

    virtual void Prepare();

    void ReplyError(const TError& error);
    void ReplySuccess(const NYTree::TYsonString& yson);
    void ReplySuccess();
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
class TTypedCommandBase
    : public virtual TCommandBase
{
public:
    virtual void Execute(ICommandContextPtr context)
    {
        Context = context;
        try {
            ParseRequest();
            
            Prepare();

            DoExecute();

            // Assume empty successful reply by default.
            if (!Replied) {
                ReplySuccess();
            }
        } catch (const std::exception& ex) {
            ReplyError(ex);
        }
    }

protected:
    TIntrusivePtr<TRequest> Request;

    virtual void DoExecute() = 0;

private:
    void ParseRequest()
    {
        Request = New<TRequest>();
        try {
            auto arguments = Context->Request().Arguments;;
            Request = NYTree::ConvertTo<TIntrusivePtr<TRequest>>(arguments);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing command arguments") << ex;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest, class = void>
class TTransactionalCommandBase
{ };

DECLARE_ENUM(EAllowNullTransaction,
    (Yes)
    (No)
);

DECLARE_ENUM(EPingTransaction,
    (Yes)
    (No)
);

template <class TRequest>
class TTransactionalCommandBase<
    TRequest,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TRequest&, TTransactionalRequest&> >::TType
>
    : public virtual TTypedCommandBase<TRequest>
{
protected:
    NTransactionClient::TTransactionId GetTransactionId(EAllowNullTransaction allowNullTransaction)
    {
        auto transaction = this->GetTransaction(allowNullTransaction, EPingTransaction::Yes);
        return transaction ? transaction->GetId() : NTransactionClient::NullTransactionId;
    }

    NTransactionClient::TTransactionPtr GetTransaction(EAllowNullTransaction allowNullTransaction, EPingTransaction pingTransaction)
    {
        if (allowNullTransaction == EAllowNullTransaction::No &&
            this->Request->TransactionId == NTransactionClient::NullTransactionId)
        {
            THROW_ERROR_EXCEPTION("Transaction is required");
        }

        auto transactionId = this->Request->TransactionId;
        if (transactionId == NTransactionClient::NullTransactionId) {
            return nullptr;
        }

        NTransactionClient::TTransactionAttachOptions options(transactionId);
        options.AutoAbort = false;
        options.Ping = (pingTransaction == EPingTransaction::Yes);
        options.PingAncestors = this->Request->PingAncestors;

        auto transactionManager = this->Context->GetTransactionManager();
        return transactionManager->Attach(options);
    }

    void SetTransactionId(NRpc::IClientRequestPtr request, EAllowNullTransaction allowNullTransaction)
    {
        NCypressClient::SetTransactionId(request, this->GetTransactionId(allowNullTransaction));
    }

};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest, class = void>
class TMutatingCommandBase
{ };

template <class TRequest>
class TMutatingCommandBase <
    TRequest,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TRequest&, TMutatingRequest&> >::TType
>
    : public virtual TTypedCommandBase<TRequest>
{
protected:
    void GenerateMutationId(NRpc::IClientRequestPtr request)
    {
        NHydra::SetMutationId(request, this->GenerateMutationId());
    }

    NHydra::TMutationId GenerateMutationId()
    {
        auto result = this->CurrentMutationId;
        if (this->CurrentMutationId != NHydra::NullMutationId) {
            ++(this->CurrentMutationId).Parts[0];
        }
        return result;
    }

private:
    NHydra::TMutationId CurrentMutationId;

    virtual void Prepare() override
    {
        TTypedCommandBase<TRequest>::Prepare();

        this->CurrentMutationId =
            this->Request->MutationId == NHydra::NullMutationId
            ? NHydra::GenerateMutationId()
            : this->Request->MutationId;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest, class = void>
class TSuppressableAccessTrackingCommmandBase
{ };

template <class TRequest>
class TSuppressableAccessTrackingCommmandBase <
    TRequest,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TRequest&, TSuppressableAccessTrackingRequest&> >::TType
>
    : public virtual TTypedCommandBase<TRequest>
{
protected:
    void SetSuppressAccessTracking(NRpc::IClientRequestPtr request)
    {
        NCypressClient::SetSuppressAccessTracking(
            &request->Header(),
            this->Request->SuppressAccessTracking);
    }

};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
class TTypedCommand
    : public virtual TTypedCommandBase<TRequest>
    , public TTransactionalCommandBase<TRequest>
    , public TMutatingCommandBase<TRequest>
    , public TSuppressableAccessTrackingCommmandBase<TRequest>
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

