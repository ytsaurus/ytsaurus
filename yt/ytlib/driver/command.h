#pragma once

#include "private.h"
#include "driver.h"

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/mpl.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct ICommand
{
    virtual ~ICommand() = default;
    virtual void Execute(ICommandContextPtr context) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ICommandContext
    : public virtual TRefCounted
{
    virtual const TDriverConfigPtr& GetConfig() = 0;
    virtual const NApi::IClientPtr& GetClient() = 0;
    virtual const IDriverPtr& GetDriver() = 0;

    virtual const TDriverRequest& Request() = 0;

    virtual const NFormats::TFormat& GetInputFormat() = 0;
    virtual const NFormats::TFormat& GetOutputFormat() = 0;

    virtual void ProduceOutputValue(const NYson::TYsonString& yson) = 0;
    virtual NYson::TYsonString ConsumeInputValue() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICommandContext)

////////////////////////////////////////////////////////////////////////////////

class TCommandBase
    : public NYTree::TYsonSerializableLite
    , public ICommand
{
protected:
    NLogging::TLogger Logger = DriverLogger;

    virtual void DoExecute(ICommandContextPtr context) = 0;

    TCommandBase()
    {
        SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::Keep);
    }

public:
    virtual void Execute(ICommandContextPtr context) override
    {
        const auto& request = context->Request();
        Logger.AddTag("RequestId: %" PRIx64 ", User: %v",
            request.Id,
            request.AuthenticatedUser);
        Deserialize(*this, request.Parameters);
        DoExecute(context);
    }
};

template <class TOptions>
class TTypedCommandBase
    : public TCommandBase
{
protected:
    TOptions Options;

};

template <class TOptions, class = void>
class TTransactionalCommandBase
{ };

template <class TOptions>
class TTransactionalCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TTransactionalOptions&>>::TType
>
    : public virtual TTypedCommandBase<TOptions>
{
protected:
    TTransactionalCommandBase()
    {
        this->RegisterParameter("transaction_id", this->Options.TransactionId)
            .Optional();
        this->RegisterParameter("ping_ancestor_transactions", this->Options.PingAncestors)
            .Optional();
        this->RegisterParameter("sticky", this->Options.Sticky)
            .Optional();
    }

    NApi::ITransactionPtr AttachTransaction(
        ICommandContextPtr context,
        bool required)
    {
        const auto& transactionId = this->Options.TransactionId;
        if (!transactionId) {
            if (required) {
                THROW_ERROR_EXCEPTION("Transaction is required");
            }
            return nullptr;
        }

        NApi::TTransactionAttachOptions options;
        options.Ping = !required;
        options.PingAncestors = this->Options.PingAncestors;
        options.Sticky = this->Options.Sticky;
        return context->GetClient()->AttachTransaction(transactionId, options);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions, class = void>
class TMutatingCommandBase
{ };

template <class TOptions>
class TMutatingCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TMutatingOptions&>>::TType
>
    : public virtual TTypedCommandBase<TOptions>
{
protected:
    TMutatingCommandBase()
    {
        this->RegisterParameter("mutation_id", this->Options.MutationId)
            .Optional();
        this->RegisterParameter("retry", this->Options.Retry)
            .Optional();
    }

};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions, class = void>
class TReadOnlyMasterCommandBase
{ };

template <class TOptions>
class TReadOnlyMasterCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TMasterReadOptions&>>::TType
>
    : public virtual TTypedCommandBase<TOptions>
{
protected:
    TReadOnlyMasterCommandBase()
    {
        this->RegisterParameter("read_from", this->Options.ReadFrom)
            .Optional();
        this->RegisterParameter("expire_after_successful_update_time", this->Options.ExpireAfterSuccessfulUpdateTime)
            .Optional();
        this->RegisterParameter("expire_after_failed_update_time", this->Options.ExpireAfterFailedUpdateTime)
            .Optional();
        this->RegisterParameter("cache_sticky_group_size", this->Options.CacheStickyGroupSize)
            .Optional();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions, class = void>
class TReadOnlyTabletCommandBase
{ };

template <class TOptions>
class TReadOnlyTabletCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TTabletReadOptions&>>::TType
>
    : public virtual TTypedCommandBase<TOptions>
{
protected:
    TReadOnlyTabletCommandBase()
    {
        this->RegisterParameter("read_from", this->Options.ReadFrom)
            .Optional();
        this->RegisterParameter("backup_request_delay", this->Options.BackupRequestDelay)
            .Optional();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions, class = void>
class TSuppressableAccessTrackingCommmandBase
{ };

template <class TOptions>
class TSuppressableAccessTrackingCommmandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TSuppressableAccessTrackingOptions&>>::TType
>
    : public virtual TTypedCommandBase<TOptions>
{
protected:
    TSuppressableAccessTrackingCommmandBase()
    {
        this->RegisterParameter("suppress_access_tracking", this->Options.SuppressAccessTracking)
            .Optional();
        this->RegisterParameter("suppress_modification_tracking", this->Options.SuppressModificationTracking)
            .Optional();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions, class = void>
class TPrerequisiteCommandBase
{ };

template <class TOptions>
class TPrerequisiteCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TPrerequisiteOptions&>>::TType
>
    : public virtual TTypedCommandBase<TOptions>
{
protected:
    TPrerequisiteCommandBase()
    {
        this->RegisterParameter("prerequisite_transaction_ids", this->Options.PrerequisiteTransactionIds)
            .Optional();
        this->RegisterParameter("prerequisite_revisions", this->Options.PrerequisiteRevisions)
            .Optional();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions, class = void>
class TTimeoutCommandBase
{ };

template <class TOptions>
class TTimeoutCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TTimeoutOptions&>>::TType
>
    : public virtual TTypedCommandBase<TOptions>
{
protected:
    TTimeoutCommandBase()
    {
        this->RegisterParameter("timeout", this->Options.Timeout)
            .Optional();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTabletReadOptions
{
    NTransactionClient::TTransactionId TransactionId;
};

template <class TOptions, class = void>
class TTabletReadCommandBase
{ };

template <class TOptions>
class TTabletReadCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, TTabletReadOptions&>>::TType
>
    : public virtual TTypedCommandBase<TOptions>
{
protected:
    TTabletReadCommandBase()
    {
        this->RegisterParameter("transaction_id", this->Options.TransactionId)
            .Optional();
    }

    NApi::IClientBasePtr GetClientBase(ICommandContextPtr context)
    {
        const auto& transactionId = this->Options.TransactionId;
        if (transactionId) {
            NApi::TTransactionAttachOptions options;
            options.Sticky = true;
            return context->GetClient()->AttachTransaction(transactionId, options);
        } else {
            return context->GetClient();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTabletWriteOptions
    : public TTabletReadOptions
{
    NTransactionClient::EAtomicity Atomicity;
    NTransactionClient::EDurability Durability;
};

struct TInsertRowsOptions
    : public TTabletWriteOptions
    , public NApi::TModifyRowsOptions
{ };

struct TDeleteRowsOptions
    : public TTabletWriteOptions
    , public NApi::TModifyRowsOptions
{ };

template <class TOptions, class = void>
class TTabletWriteCommandBase
{ };

template <class TOptions>
class TTabletWriteCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, TTabletWriteOptions&>>::TType
>
    : public virtual TTypedCommandBase<TOptions>
{
protected:
    TTabletWriteCommandBase()
    {
        this->RegisterParameter("atomicity", this->Options.Atomicity)
            .Default();
        this->RegisterParameter("durability", this->Options.Durability)
            .Default();
    }

    NApi::ITransactionPtr GetTransaction(ICommandContextPtr context)
    {
        const auto& transactionId = this->Options.TransactionId;
        if (transactionId) {
            NApi::TTransactionAttachOptions options;
            options.Sticky = true;
            return context->GetClient()->AttachTransaction(transactionId, options);
        } else {
            NApi::TTransactionStartOptions options;
            options.Atomicity = this->Options.Atomicity;
            options.Durability = this->Options.Durability;
            auto asyncResult = context->GetClient()->StartTransaction(NTransactionClient::ETransactionType::Tablet, options);
            return NConcurrency::WaitFor(asyncResult)
                .ValueOrThrow();
        }
    }

    bool ShouldCommitTransaction()
    {
        return !this->Options.TransactionId;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
class TTypedCommand
    : public virtual TTypedCommandBase<TOptions>
    , public TTransactionalCommandBase<TOptions>
    , public TTabletReadCommandBase<TOptions>
    , public TTabletWriteCommandBase<TOptions>
    , public TMutatingCommandBase<TOptions>
    , public TReadOnlyMasterCommandBase<TOptions>
    , public TReadOnlyTabletCommandBase<TOptions>
    , public TSuppressableAccessTrackingCommmandBase<TOptions>
    , public TPrerequisiteCommandBase<TOptions>
    , public TTimeoutCommandBase<TOptions>
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

