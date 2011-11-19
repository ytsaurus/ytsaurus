#pragma once

#include "common.h"
#include "chunk_manager.h"

#include "../cypress/cypress_manager.h"
#include "../cypress/node.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandler::TPtr CreateChunkMapTypeHandler(
    NCypress::TCypressManager* cypressManager,
    TChunkManager* chunkManager);

NCypress::INodeTypeHandler::TPtr CreateChunkListMapTypeHandler(
    NCypress::TCypressManager* cypressManager,
    TChunkManager* chunkManager);

IHolderRegistry::TPtr CreateHolderRegistry(
    NCypress::TCypressManager* cypressManager);

NCypress::INodeTypeHandler::TPtr CreateHolderTypeHandler(
    NCypress::TCypressManager* cypressManager,
    TChunkManager* chunkManager);

NCypress::INodeTypeHandler::TPtr CreateHolderMapTypeHandler(
    NCypress::TCypressManager* cypressManager,
    TChunkManager* chunkManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
