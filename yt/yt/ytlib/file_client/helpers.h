#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NFileClient {

////////////////////////////////////////////////////////////////////////////////

// TODO(achains): Move to chunk_client/helpers.h?
NChunkClient::TMultiChunkWriterOptionsPtr GetWriterOptions(
    const NYTree::IAttributeDictionaryPtr& attributes,
    const NYPath::TRichYPath& path,
    const IMemoryUsageTrackerPtr& tracker);

////////////////////////////////////////////////////////////////////////////////

struct TFetchFileObjectInfoOptions
{
    bool SuppressAccessTracking = false;
    bool SuppressExpirationTimeoutRenewal = false;
};

struct TFileObjectInfo
{
    NChunkClient::TUserObject UserObject;
    NHydra::TRevision Revision = NHydra::NullRevision;
    int ChunkCount = 0;
};

//! Fetches basic and extended attributes of a file node.
TFileObjectInfo FetchFileObjectInfo(
    const NApi::NNative::IClientPtr& client,
    const NYPath::TYPath& path,
    NTransactionClient::TTransactionId transactionId,
    const TFetchFileObjectInfoOptions& options,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
