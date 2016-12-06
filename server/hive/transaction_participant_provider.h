#pragma once

#include "public.h"

#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/object_client/public.h>

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
    NObjectClient::TCellTag cellTag,
    NHiveClient::TCellDirectoryPtr cellDirectory);

ITransactionParticipantProviderPtr CreateTransactionParticipantProvider(
    NApi::INativeConnectionPtr connection);

ITransactionParticipantProviderPtr CreateTransactionParticipantProvider(
    NHiveClient::TClusterDirectoryPtr clusterDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
