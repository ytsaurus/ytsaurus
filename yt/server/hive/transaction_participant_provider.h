#pragma once

#include "public.h"

#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

struct ITransactionParticipantProvider
    : public virtual TRefCounted
{
    virtual NHiveClient::ITransactionParticipantPtr TryCreate(
        const TCellId& cellId,
        const NApi::TTransactionParticipantOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransactionParticipantProvider)

////////////////////////////////////////////////////////////////////////////////

ITransactionParticipantProviderPtr CreateTransactionParticipantProvider(
    NHiveClient::TCellDirectoryPtr cellDirectory,
    NTransactionClient::ITimestampProviderPtr timestampProvider,
    NObjectClient::TCellTag cellTag);

ITransactionParticipantProviderPtr CreateTransactionParticipantProvider(
    NApi::NNative::IConnectionPtr connection);

ITransactionParticipantProviderPtr CreateTransactionParticipantProvider(
    NHiveClient::TClusterDirectoryPtr clusterDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
