#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

struct TFileYPathProxy
    : public NChunkClient::TChunkOwnerYPathProxy
{
    DEFINE_YPATH_PROXY(File);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
