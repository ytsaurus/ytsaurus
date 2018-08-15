#pragma once
#ifndef COMMAND_INL_H
#error "Direct inclusion of this file is not allowed, include command.h"
#endif

#include "private.h"
#include "driver.h"

#include <yt/core/logging/log.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/serialize.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void ProduceSingleOutputValue(
    ICommandContextPtr context,
    TStringBuf name,
    const T& value)
{
    ProduceSingleOutput(context, name, [&](NYson::IYsonConsumer* consumer) {
        NYTree::BuildYsonFluently(consumer)
            .Value(value);
    });
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
TTransactionalCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TTransactionalOptions&>>::TType
>::TTransactionalCommandBase()
{
    this->RegisterParameter("transaction_id", this->Options.TransactionId)
        .Optional();
    this->RegisterParameter("ping_ancestor_transactions", this->Options.PingAncestors)
        .Optional();
    this->RegisterParameter("sticky", this->Options.Sticky)
        .Optional();
}

template <class TOptions>
NApi::ITransactionPtr TTransactionalCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TTransactionalOptions&>>::TType
>::AttachTransaction(
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

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
TMutatingCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TMutatingOptions&>>::TType
>::TMutatingCommandBase()
{
    this->RegisterParameter("mutation_id", this->Options.MutationId)
        .Optional();
    this->RegisterParameter("retry", this->Options.Retry)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
TReadOnlyMasterCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TMasterReadOptions&>>::TType
>::TReadOnlyMasterCommandBase()
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

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
TReadOnlyTabletCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TTabletReadOptions&>>::TType
>::TReadOnlyTabletCommandBase()
{
    this->RegisterParameter("read_from", this->Options.ReadFrom)
        .Optional();
    this->RegisterParameter("backup_request_delay", this->Options.BackupRequestDelay)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
TSuppressableAccessTrackingCommmandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TSuppressableAccessTrackingOptions&>>::TType
>::TSuppressableAccessTrackingCommmandBase()
{
    this->RegisterParameter("suppress_access_tracking", this->Options.SuppressAccessTracking)
        .Optional();
    this->RegisterParameter("suppress_modification_tracking", this->Options.SuppressModificationTracking)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
TPrerequisiteCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TPrerequisiteOptions&>>::TType
>::TPrerequisiteCommandBase()
{
    this->RegisterParameter("prerequisite_transaction_ids", this->Options.PrerequisiteTransactionIds)
        .Optional();
    this->RegisterParameter("prerequisite_revisions", this->Options.PrerequisiteRevisions)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
TTimeoutCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, NApi::TTimeoutOptions&>>::TType
>::TTimeoutCommandBase()
{
    this->RegisterParameter("timeout", this->Options.Timeout)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
TTabletReadCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, TTabletReadOptions&>>::TType
>::TTabletReadCommandBase()
{
    this->RegisterParameter("transaction_id", this->Options.TransactionId)
        .Optional();
}

template <class TOptions>
NApi::IClientBasePtr TTabletReadCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, TTabletReadOptions&>>::TType
>::GetClientBase(ICommandContextPtr context)
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

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
TTabletWriteCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, TTabletWriteOptions&>>::TType
>::TTabletWriteCommandBase()
{
    this->RegisterParameter("atomicity", this->Options.Atomicity)
        .Default();
    this->RegisterParameter("durability", this->Options.Durability)
        .Default();
}

template <class TOptions>
NApi::ITransactionPtr TTabletWriteCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, TTabletWriteOptions&>>::TType
>::GetTransaction(ICommandContextPtr context)
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

template <class TOptions>
bool TTabletWriteCommandBase<
    TOptions,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<TOptions&, TTabletWriteOptions&>>::TType
>::ShouldCommitTransaction()
{
    return !this->Options.TransactionId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
