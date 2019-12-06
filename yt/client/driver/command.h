#pragma once

#include "private.h"
#include "driver.h"

#include <yt/client/api/client.h>
#include <yt/client/api/transaction.h>
#include <yt/client/api/sticky_transaction_pool.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/mpl.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NDriver {

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
    virtual TDriverRequest& MutableRequest() = 0;

    virtual const NFormats::TFormat& GetInputFormat() = 0;
    virtual const NFormats::TFormat& GetOutputFormat() = 0;

    virtual void ProduceOutputValue(const NYson::TYsonString& yson) = 0;
    virtual NYson::TYsonString ConsumeInputValue() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICommandContext)

////////////////////////////////////////////////////////////////////////////////

//! Calls |context->ProduceOutputValue| with |TYsonString| created by the producer.
void ProduceOutput(
    ICommandContextPtr context,
    const std::function<void(NYson::IYsonConsumer*)>& producer);

//! Produces either nothing (v3) or empty map (>=v4).
void ProduceEmptyOutput(ICommandContextPtr context);

//! Run |producer| (v3) or open a map with key |name| and run |producer| then close map (>=v4).
void ProduceSingleOutput(
    ICommandContextPtr context,
    TStringBuf name,
    const std::function<void(NYson::IYsonConsumer*)>& producer);

//! Produces either |value| (v3) or map {|name|=|value|} (>=v4).
template <typename T>
void ProduceSingleOutputValue(
    ICommandContextPtr context,
    TStringBuf name,
    const T& value);

////////////////////////////////////////////////////////////////////////////////

class TCommandBase
    : public NYTree::TYsonSerializableLite
    , public ICommand
{
protected:
    NLogging::TLogger Logger = DriverLogger;

    virtual void DoExecute(ICommandContextPtr context) = 0;

    TCommandBase();

    void ProduceResponseParameters(
        ICommandContextPtr context,
        const std::function<void(NYson::IYsonConsumer*)>& producer);

    bool ValidateSuperuserPermissions(const ICommandContextPtr& context) const;

    std::optional<bool> RewriteOperationPathOption;
    bool RewriteOperationPath = true;

public:
    virtual void Execute(ICommandContextPtr context) override;
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
    TTransactionalCommandBase();
    NApi::ITransactionPtr AttachTransaction(
        ICommandContextPtr context,
        bool required);
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
    TMutatingCommandBase();
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
    TReadOnlyMasterCommandBase();
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
    TReadOnlyTabletCommandBase();
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions, class = void>
class TSuppressableAccessTrackingCommandBase
{ };

template <class TOptions>
class TSuppressableAccessTrackingCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TSuppressableAccessTrackingOptions&>>::TType
>
    : public virtual TTypedCommandBase<TOptions>
{
protected:
    TSuppressableAccessTrackingCommandBase();
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
    TPrerequisiteCommandBase();
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
    TTimeoutCommandBase();
};

////////////////////////////////////////////////////////////////////////////////

struct TTabletTransactionOptions
{
    NTransactionClient::TTransactionId TransactionId;
};

template <class TOptions, class = void>
class TTabletReadCommandBase
{ };

template <class TOptions>
class TTabletReadCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, TTabletTransactionOptions&>>::TType
>
    : public virtual TTypedCommandBase<TOptions>
{
protected:
    TTabletReadCommandBase();
    NApi::IClientBasePtr GetClientBase(ICommandContextPtr context);
};

////////////////////////////////////////////////////////////////////////////////

struct TTabletWriteOptions
    : public TTabletTransactionOptions
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

struct TLockRowsOptions
    : public TTabletTransactionOptions
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
    TTabletWriteCommandBase();
    NApi::ITransactionPtr GetTransaction(ICommandContextPtr context);
    bool ShouldCommitTransaction();
};

template <class TOptions, class = void>
class TSelectRowsCommandBase
{ };

template <class TOptions>
class TSelectRowsCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TSelectRowsOptionsBase&>>::TType
>
    : public virtual TTypedCommandBase<TOptions>
{
protected:
    TSelectRowsCommandBase();
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
    , public TSuppressableAccessTrackingCommandBase<TOptions>
    , public TPrerequisiteCommandBase<TOptions>
    , public TTimeoutCommandBase<TOptions>
    , public TSelectRowsCommandBase<TOptions>
{ };

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath RewritePath(const NYPath::TYPath& path, bool rewriteOperationPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

#define COMMAND_INL_H
#include "command-inl.h"
#undef COMMAND_INL_H
