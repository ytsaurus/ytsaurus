#pragma once

#include "public.h"
#include "private.h"
#include "driver.h"

#include <yt/ytlib/api/client.h>

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

struct ICommandContext
    : public virtual TRefCounted
{
    virtual TDriverConfigPtr GetConfig() = 0;
    virtual NApi::IClientPtr GetClient() = 0;
    virtual IDriverPtr GetDriver() = 0;

    virtual const TDriverRequest& Request() = 0;

    virtual const NFormats::TFormat& GetInputFormat() = 0;
    virtual const NFormats::TFormat& GetOutputFormat() = 0;

    virtual void ProduceOutputValue(const NYson::TYsonString& yson) = 0;
    virtual NYson::TYsonString ConsumeInputValue() = 0;

    virtual void PinTransaction(NApi::ITransactionPtr transaction, TDuration timeout) = 0;
    virtual bool UnpinTransaction(const NTransactionClient::TTransactionId& transactionId) = 0;
    virtual NApi::ITransactionPtr FindAndTouchTransaction(const NTransactionClient::TTransactionId& transactionId) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICommandContext)

////////////////////////////////////////////////////////////////////////////////

class TCommandBase
    : public NYTree::TYsonSerializableLite
{
public:
    NLogging::TLogger& GetLogger()
    {
        return Logger;
    }

protected:
    NLogging::TLogger Logger = DriverLogger;

    TCommandBase()
    {
        SetKeepOptions(true);
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
            .Alias("success_expiration_time")
            .Optional();
        this->RegisterParameter("expire_after_failed_update_time", this->Options.ExpireAfterFailedUpdateTime)
            .Alias("failure_expiration_time")
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

template <class TOptions>
class TTypedCommand
    : public virtual TTypedCommandBase<TOptions>
    , public TTransactionalCommandBase<TOptions>
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

