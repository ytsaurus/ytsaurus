#pragma once

#include "public.h"

#include <yt/server/master/chunk_server/chunk_owner_base.h>

#include <yt/server/master/cypress_server/node_detail.h>

namespace NYT::NJournalServer {

////////////////////////////////////////////////////////////////////////////////

class TJournalNode
    : public NChunkServer::TChunkOwnerBase
{
public:
    using TBase = NChunkServer::TChunkOwnerBase;

    DEFINE_BYVAL_RW_PROPERTY(int, ReadQuorum);
    DEFINE_BYVAL_RW_PROPERTY(int, WriteQuorum);

public:
    using TBase::TBase;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    virtual void BeginUpload(const TBeginUploadContext& context) override;

    NChunkServer::TChunk* GetTrailingChunk() const;

    TJournalNode* GetTrunkNode();
    const TJournalNode* GetTrunkNode() const;

    bool GetSealed() const;
    void SetSealed(bool value);

    virtual NSecurityServer::TClusterResources GetDeltaResourceUsage() const override;
    virtual NSecurityServer::TClusterResources GetTotalResourceUsage() const override;

private:
    // Only maintained at trunk nodes.
    bool Sealed_ = true;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalServer

