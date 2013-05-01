#pragma once

#include "public.h"

#include <ytlib/file_client/file_ypath.pb.h>

#include <ytlib/chunk_client/chunk_owner_ypath_proxy.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

struct TFileYPathProxy
    : NChunkClient::TChunkOwnerYPathProxy
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
