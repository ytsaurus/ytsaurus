#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/ytlib/journal_client/journal_ypath.pb.h>

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
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalClient
} // namespace NYT
