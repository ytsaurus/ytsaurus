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

////////////////////////////////////////////////////////////////////////////////

struct TMutatingRequest
    : public virtual TRequest
{
    NRpc::TMutationId MutationId;
    bool Retry;

    TMutatingRequest()
    {
        RegisterParameter("mutation_id", MutationId)
            .Default(NRpc::NullMutationId);
        RegisterParameter("retry", Retry)
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TReadOnlyRequest
    : public virtual TRequest
{
    NApi::EMasterChannelKind ReadFrom;

    TReadOnlyRequest()
    {
        RegisterParameter("read_from", ReadFrom)
            .Default(NApi::EMasterChannelKind::LeaderOrFollower);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSuppressableAccessTrackingRequest
    : public virtual TRequest
{
    bool SuppressAccessTracking;
    bool SuppressModificationTracking;

    TSuppressableAccessTrackingRequest()
    {
        RegisterParameter("suppress_access_tracking", SuppressAccessTracking)
            .Default(false);
        RegisterParameter("suppress_modification_tracking", SuppressModificationTracking)
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TPrerequisiteRequest
    : public virtual TRequest
{
    std::vector<NObjectClient::TTransactionId> PrerequisiteTransactionIds;

    TPrerequisiteRequest()
    {
        RegisterParameter("prerequisite_transaction_ids", PrerequisiteTransactionIds)
            .Default();
    }
};

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

template <class TRequest>
class TTransactionalCommandBase<
    TRequest,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TRequest&, TTransactionalRequest&>>::TType
>
    : public virtual TTypedCommandBase<TRequest>
{
protected:
    NTransactionClient::TTransactionPtr AttachTransaction(bool required)
    {
        const auto& transactionId = this->Request_->TransactionId;
        if (transactionId == NTransactionClient::NullTransactionId) {
            if (required) {
                THROW_ERROR_EXCEPTION("Transaction is required");
            }
            return nullptr;
        }

        auto transactionManager = this->Context_->GetClient()->GetTransactionManager();

        NTransactionClient::TTransactionAttachOptions options;
        options.Ping = !required;
        options.PingAncestors = this->Request_->PingAncestors;
        return transactionManager->Attach(transactionId, options);
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
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TRequest&, TMutatingRequest&>>::TType
>
    : public virtual TTypedCommandBase<TRequest>
{
protected:
    void SetMutatingOptions(NApi::TMutatingOptions* options)
    {
        options->MutationId = this->CurrentMutationId_;
        ++(this->CurrentMutationId_).Parts32[0];

        options->Retry = this->Request_->Retry;
    }

private:
    NRpc::TMutationId CurrentMutationId_;

    virtual void Prepare() override
    {
        TTypedCommandBase<TRequest>::Prepare();

        this->CurrentMutationId_ = this->Request_->MutationId == NRpc::NullMutationId
            ? NRpc::GenerateMutationId()
            : this->Request_->MutationId;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest, class = void>
class TReadOnlyCommandBase
{ };

template <class TRequest>
class TReadOnlyCommandBase <
    TRequest,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TRequest&, TReadOnlyRequest&>>::TType
>
    : public virtual TTypedCommandBase<TRequest>
{
protected:
    void SetReadOnlyOptions(NApi::TReadOnlyOptions* options)
    {
        options->ReadFrom = this->Request_->ReadFrom;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest, class = void>
class TSuppressableAccessTrackingCommmandBase
{ };

template <class TRequest>
class TSuppressableAccessTrackingCommmandBase <
    TRequest,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TRequest&, TSuppressableAccessTrackingRequest&>>::TType
>
    : public virtual TTypedCommandBase<TRequest>
{
protected:
    void SetSuppressableAccessTrackingOptions(NApi::TSuppressableAccessTrackingOptions* options)
    {
        options->SuppressAccessTracking = this->Request_->SuppressAccessTracking;
        options->SuppressModificationTracking = this->Request_->SuppressModificationTracking;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest, class = void>
class TPrerequisiteCommandBase
{ };

template <class TRequest>
class TPrerequisiteCommandBase <
    TRequest,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TRequest&, TPrerequisiteRequest&>>::TType
>
    : public virtual TTypedCommandBase<TRequest>
{
protected:
    void SetPrerequisites(NApi::TPrerequisiteOptions* options)
    {
        options->PrerequisiteTransactionIds = this->Request_->PrerequisiteTransactionIds;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
class TTypedCommand
    : public virtual TTypedCommandBase<TRequest>
    , public TTransactionalCommandBase<TRequest>
    , public TMutatingCommandBase<TRequest>
    , public TReadOnlyCommandBase<TRequest>
    , public TSuppressableAccessTrackingCommmandBase<TRequest>
    , public TPrerequisiteCommandBase<TRequest>
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

