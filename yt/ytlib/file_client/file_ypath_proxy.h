#pragma once

#include "public.h"

#include <ytlib/chunk_client/chunk_owner_ypath_proxy.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

struct TFileYPathProxy
    : public NChunkClient::TChunkOwnerYPathProxy
{
    static Stroka GetServiceName()
    {
        return "File";
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
