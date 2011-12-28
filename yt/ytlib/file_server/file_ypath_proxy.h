#pragma once

#include "common.h"
#include "file_ypath.pb.h"

#include "../misc/configurable.h"
#include "../ytree/ypath_proxy.h"
#include "../chunk_server/common.h"

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

struct TFileManifest
    : public TConfigurable
{
    NChunkServer::TChunkId ChunkId;

    TFileManifest()
    {
        Register("chunk_id", ChunkId);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFileYPathProxy
    : NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Fetch);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT
