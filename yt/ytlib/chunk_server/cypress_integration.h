#pragma once

#include "common.h"
#include "chunk_manager.h"

#include <ytlib/meta_state/meta_state_manager.h>
#include <ytlib/cypress/cypress_manager.h>
#include <ytlib/cypress/node.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandler::TPtr CreateChunkMapTypeHandler(
    NCypress::TCypressManager* cypressManager,
    TChunkManager* chunkManager);

NCypress::INodeTypeHandler::TPtr CreateLostChunkMapTypeHandler(
    NCypress::TCypressManager* cypressManager,
    TChunkManager* chunkManager);

NCypress::INodeTypeHandler::TPtr CreateOverreplicatedChunkMapTypeHandler(
    NCypress::TCypressManager* cypressManager,
    TChunkManager* chunkManager);

NCypress::INodeTypeHandler::TPtr CreateUnderreplicatedChunkMapTypeHandler(
    NCypress::TCypressManager* cypressManager,
    TChunkManager* chunkManager);

NCypress::INodeTypeHandler::TPtr CreateChunkListMapTypeHandler(
    NCypress::TCypressManager* cypressManager,
    TChunkManager* chunkManager);

IHolderAuthority::TPtr CreateHolderAuthority(
    NCypress::TCypressManager* cypressManager);

NCypress::INodeTypeHandler::TPtr CreateHolderTypeHandler(
    NCypress::TCypressManager* cypressManager,
    TChunkManager* chunkManager);

NCypress::INodeTypeHandler::TPtr CreateHolderMapTypeHandler(
    NMetaState::IMetaStateManager* metaStateManager,
    NCypress::TCypressManager* cypressManager,
    TChunkManager* chunkManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
