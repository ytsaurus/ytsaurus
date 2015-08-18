#pragma once

#include "public.h"

#include <ytlib/journal_client/journal_ypath.pb.h>

#include <ytlib/chunk_client/chunk_owner_ypath_proxy.h>

namespace NYT {
namespace NJournalClient {

////////////////////////////////////////////////////////////////////////////////

struct TJournalYPathProxy
    : public NChunkClient::TChunkOwnerYPathProxy
{
    static Stroka GetServiceName()
    {
        return "Journal";
    }

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Seal);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalClient
} // namespace NYT
