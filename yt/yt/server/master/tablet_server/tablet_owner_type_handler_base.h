#pragma once

#include "public.h"

#include <yt/yt/server/master/chunk_server/chunk_owner_type_handler.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TTabletOwnerTypeHandlerBase
    : public NChunkServer::TChunkOwnerTypeHandler<TImpl>
{
private:
    using TBase = NChunkServer::TChunkOwnerTypeHandler<TImpl>;

public:
    using TBase::TBase;
    explicit TTabletOwnerTypeHandlerBase(NCellMaster::TBootstrap* bootstrap)
        : TBase(bootstrap)
    {
        // NB: Due to virtual inheritance bootstrap has to be explicitly initialized.
        this->SetBootstrap(bootstrap);
    }

    bool IsSupportedInheritableAttribute(const TString& key) const override;

protected:
    void DoDestroy(TImpl* owner) override;

    void DoClone(
        TImpl* sourceNode,
        TImpl* clonedTrunkNode,
        NCypressServer::ICypressNodeFactory* factory,
        NCypressServer::ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;
    void DoBeginCopy(
        TImpl* node,
        NCypressServer::TBeginCopyContext* context) override;
    void DoEndCopy(
        TImpl* node,
        NCypressServer::TEndCopyContext* context,
        NCypressServer::ICypressNodeFactory* factory) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
