#pragma once

#include "private.h"
#include "public.h"
#include "driver.h"

#include <core/misc/error.h>
#include <core/misc/mpl.h>

#include <core/ytree/yson_serializable.h>
#include <core/ytree/convert.h>

#include <core/yson/consumer.h>
#include <core/yson/parser.h>
#include <core/yson/writer.h>

#include <core/rpc/channel.h>
#include <core/rpc/helpers.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/security_client/public.h>

#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/api/connection.h>
#include <ytlib/api/client.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TRequest
    : public virtual NYTree::TYsonSerializable
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
    NRpc::TMutationId MutationId;

    TMutatingRequest()
    {
        RegisterParameter("mutation_id", MutationId)
            .Default(NRpc::NullMutationId);
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
    : public virtual TRefCounted
{
    virtual TDriverConfigPtr GetConfig() = 0;
    virtual NApi::IClientPtr GetClient() = 0;

    virtual const TDriverRequest& Request() const = 0;

    virtual const NFormats::TFormat& GetInputFormat() = 0;
    virtual const NFormats::TFormat& GetOutputFormat() = 0;

    virtual NYTree::TYsonProducer CreateInputProducer() = 0;
    virtual std::unique_ptr<NYson::IYsonConsumer> CreateOutputConsumer() = 0;

    virtual void Reply(const TError& error) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICommandContext)

///////////////////////////////////////////////////////////////////////////////

struct ICommand
    : public TRefCounted
{
    virtual void Execute(ICommandContextPtr context) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICommand)

////////////////////////////////////////////////////////////////////////////////

class TCommandBase
    : public ICommand
{
protected:
    ICommandContextPtr Context_ = nullptr;
    bool Replied_ = false;


    virtual void Prepare();

    void Reply(const TError& error);
    void Reply(const NYTree::TYsonString& yson);
    void Reply();

};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
class TTypedCommandBase
    : public virtual TCommandBase
{
public:
    virtual void Execute(ICommandContextPtr context)
    {
        Context_ = context;
        try {
            ParseRequest();

            Prepare();

            DoExecute();

            // Assume empty successful reply by default.
            if (!Replied_) {
                Reply();
            }
        } catch (const std::exception& ex) {
            Reply(ex);
        }
    }

protected:
    TIntrusivePtr<TRequest> Request_;

    virtual void DoExecute() = 0;

private:
    void ParseRequest()
    {
        Request_ = New<TRequest>();
        try {
            auto parameters = Context_->Request().Parameters;
            Request_ = NYTree::ConvertTo<TIntrusivePtr<TRequest>>(parameters);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing command arguments") << ex;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest, class = void>
class TTransactionalCommandBase
{ };

DEFINE_ENUM(EAllowNullTransaction,
    (Yes)
    (No)
);

DEFINE_ENUM(EPingTransaction,
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
            this->Request_->TransactionId == NTransactionClient::NullTransactionId)
        {
            THROW_ERROR_EXCEPTION("Transaction is required");
        }

        auto transactionId = this->Request_->TransactionId;
        if (transactionId == NTransactionClient::NullTransactionId) {
            return nullptr;
        }

        NTransactionClient::TTransactionAttachOptions options(transactionId);
        options.AutoAbort = false;
        options.Ping = (pingTransaction == EPingTransaction::Yes);
        options.PingAncestors = this->Request_->PingAncestors;

        auto transactionManager = this->Context_->GetClient()->GetTransactionManager();
        return transactionManager->Attach(options);
    }

    void SetTransactionalOptions(NApi::TTransactionalOptions* options)
    {
        options->TransactionId = this->Request_->TransactionId;
        options->PingAncestors = this->Request_->PingAncestors;
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
        NRpc::SetMutationId(request, this->GenerateMutationId());
    }

    void SetMutatingOptions(NApi::TMutatingOptions* options)
    {
        options->MutationId = this->GenerateMutationId();
    }

    NRpc::TMutationId GenerateMutationId()
    {
        auto result = this->CurrentMutationId;
        if (this->CurrentMutationId != NRpc::NullMutationId) {
            ++(this->CurrentMutationId).Parts32[0];
        }
        return result;
    }

private:
    NRpc::TMutationId CurrentMutationId;

    virtual void Prepare() override
    {
        TTypedCommandBase<TRequest>::Prepare();

        this->CurrentMutationId =
            this->Request_->MutationId == NRpc::NullMutationId
            ? NRpc::GenerateMutationId()
            : this->Request_->MutationId;
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
    void SetSuppressableAccessTrackingOptions(NApi::TSuppressableAccessTrackingOptions* options)
    {
        options->SuppressAccessTracking = this->Request_->SuppressAccessTracking;
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

