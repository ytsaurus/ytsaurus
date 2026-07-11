#pragma once

#include "public.h"

#include <yt/yt/server/lib/nbd/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/logging/public.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateJournalBlockDevice(
    std::string deviceId,
    TJournalBlockDeviceConfigPtr deviceConfig,
    TJournalBlockDeviceOptionsPtr storeOptions,
    NObjectClient::TTransactionId transactionId,
    NChunkClient::TChunkListId chunkListId,
    NApi::NNative::IClientPtr client,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
