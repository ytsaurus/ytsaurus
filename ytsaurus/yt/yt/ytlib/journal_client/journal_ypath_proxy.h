#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/yt/ytlib/journal_client/proto/journal_ypath.pb.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

struct TJournalYPathProxy
    : public NChunkClient::TChunkOwnerYPathProxy
{
    DEFINE_YPATH_PROXY(Journal);

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, UpdateStatistics);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Seal);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Truncate);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
