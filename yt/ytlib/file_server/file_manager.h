#pragma once

#include "common.h"
#include "file_node_proxy.h"
#include "file_manager.pb.h"

#include "../chunk_server/chunk.h"
#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/meta_change.h"

namespace NYT {
namespace NFileServer {

using NMetaState::TMetaChange;
using NCypress::TCypressManager;

////////////////////////////////////////////////////////////////////////////////
    
//! Manages files.
class TFileManager
    : public NMetaState::TMetaStatePart
{
public:
    typedef TIntrusivePtr<TFileManager> TPtr;

    //! Creates an instance.
    TFileManager(
        NMetaState::TMetaStateManager::TPtr metaStateManager,
        NMetaState::TCompositeMetaState::TPtr metaState,
        TCypressManager::TPtr cypressManager);

    TMetaChange<TVoid>::TPtr InitiateSetFileChunk(
        const TTransactionId& transactionId,
        const TNodeId& nodeId,
        const TChunkId& chunkId);

private:
    typedef TFileManager TThis;

    TCypressManager::TPtr CypressManager;

    virtual Stroka GetPartName() const;

    TVoid SetFileChunk(const NProto::TMsgSetFileChunk& message);

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT
