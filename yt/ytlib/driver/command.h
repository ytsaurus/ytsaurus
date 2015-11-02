#pragma once

#include "private.h"
#include "public.h"
#include "driver.h"

#include <core/misc/mpl.h>
#include <core/misc/error.h>
#include <core/ytree/convert.h>
#include <core/ytree/yson_serializable.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/security_client/public.h>

#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/api/client.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct ICommandContext
    : public virtual TRefCounted
{
    virtual TDriverConfigPtr GetConfig() = 0;
    virtual NApi::IClientPtr GetClient() = 0;

    virtual const TDriverRequest& Request() const = 0;

    virtual const NFormats::TFormat& GetInputFormat() = 0;
    virtual const NFormats::TFormat& GetOutputFormat() = 0;

    virtual void ProduceOutputValue(const NYTree::TYsonString& yson) = 0;
    virtual NYTree::TYsonString ConsumeInputValue() = 0;

};

DEFINE_REFCOUNTED_TYPE(ICommandContext)

////////////////////////////////////////////////////////////////////////////////

class TCommandBase
    : public NYTree::TYsonSerializableLite
{
protected:
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

    NTransactionClient::TTransactionPtr AttachTransaction(
        bool required,
        NTransactionClient::TTransactionManagerPtr transactionManager)
    {
        const auto& transactionId = this->Options.TransactionId;
        if (!transactionId) {
            if (required) {
                THROW_ERROR_EXCEPTION("Transaction is required");
            }
            return nullptr;
        }

        NTransactionClient::TTransactionAttachOptions options;
        options.Ping = !required;
        options.PingAncestors = this->Options.PingAncestors;
        return transactionManager->Attach(transactionId, options);
    }

};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions, class = void>
class TMutatingCommandBase
{ };

template <class TOptions>
class TMutatingCommandBase <
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
class TReadOnlyCommandBase
{ };

template <class TOptions>
class TReadOnlyCommandBase <
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TReadOnlyOptions&>>::TType
>
    : public virtual TTypedCommandBase<TOptions>
{
protected:
    TReadOnlyCommandBase()
    {
        this->RegisterParameter("read_from", this->Options.ReadFrom)
            .Optional();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions, class = void>
class TSuppressableAccessTrackingCommmandBase
{ };

template <class TOptions>
class TSuppressableAccessTrackingCommmandBase <
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
class TPrerequisiteCommandBase <
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
    }
};

template <class TOptions>
class TTypedCommand
    : public virtual TTypedCommandBase<TOptions>
    , public TTransactionalCommandBase<TOptions>
    , public TMutatingCommandBase<TOptions>
    , public TReadOnlyCommandBase<TOptions>
    , public TSuppressableAccessTrackingCommmandBase<TOptions>
    , public TPrerequisiteCommandBase<TOptions>
{ };

} // namespace NDriver
} // namespace NYT

