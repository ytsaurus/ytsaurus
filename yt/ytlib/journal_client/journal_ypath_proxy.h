#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/ytlib/journal_client/journal_ypath.pb.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

struct TJournalYPathProxy
    : public NChunkClient::TChunkOwnerYPathProxy
{
    DEFINE_YPATH_PROXY(Journal);

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Seal);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
