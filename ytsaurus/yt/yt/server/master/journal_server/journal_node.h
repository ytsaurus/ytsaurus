#pragma once

#include "public.h"

#include <yt/yt/server/master/chunk_server/chunk_owner_base.h>

#include <yt/yt/server/master/cypress_server/node_detail.h>

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

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

    void BeginUpload(const TBeginUploadContext& context) override;

    TJournalNode* GetTrunkNode();
    const TJournalNode* GetTrunkNode() const;

    bool GetSealed() const;
    void SetSealed(bool value);

    NSecurityServer::TClusterResources GetDeltaResourceUsage() const override;
    NSecurityServer::TClusterResources GetTotalResourceUsage() const override;

private:
    // Only maintained at trunk nodes.
    bool Sealed_ = true;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalServer

