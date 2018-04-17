#pragma once

#include "private.h"
#include "driver.h"

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/mpl.h>

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
    virtual TDriverRequest& MutableRequest() = 0;

    virtual const NFormats::TFormat& GetInputFormat() = 0;
    virtual const NFormats::TFormat& GetOutputFormat() = 0;

    virtual void ProduceOutputValue(const NYson::TYsonString& yson) = 0;
    virtual NYson::TYsonString ConsumeInputValue() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICommandContext)

////////////////////////////////////////////////////////////////////////////////

//! Depending on the driver API version calls context->ProduceOutputValue
//! with TYsonString created by the appropriate producer.
void ProduceOutput(
    ICommandContextPtr context,
    std::function<void(NYson::IYsonConsumer*)> producerV3,
    std::function<void(NYson::IYsonConsumer*)> producerV4);

//! Produces either nothing (v3) or empty map (v4).
void ProduceEmptyOutput(ICommandContextPtr context);

//! Run |producer| (v3) or open a map with key |name| and run |producer| then close map (v4).
void ProduceSingleOutput(
    ICommandContextPtr context,
    const TStringBuf& name,
    std::function<void(NYson::IYsonConsumer*)> producer);

//! Produces either |value| (v3) or map {|name|=|value|} (v4).
template <typename T>
void ProduceSingleOutputValue(
    ICommandContextPtr context,
    const TStringBuf& name,
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
    TSuppressableAccessTrackingCommmandBase();
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
    TTabletReadCommandBase();
    NApi::IClientBasePtr GetClientBase(ICommandContextPtr context);
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
    TTabletWriteCommandBase();
    NApi::ITransactionPtr GetTransaction(ICommandContextPtr context);
    bool ShouldCommitTransaction();
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

#define COMMAND_INL_H
#include "command-inl.h"
#undef COMMAND_INL_H
