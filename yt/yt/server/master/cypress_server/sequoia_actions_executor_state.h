#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/automaton.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/transaction_server/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TSequoiaActionsExecutorState
    : public NCellMaster::TMasterAutomatonPart
{
public:
    explicit TSequoiaActionsExecutorState(NCellMaster::TBootstrap* bootstrap);

protected:
    struct TPreparedNodeModification
    {
        TVersionedNodeId SequoiaNodeId;
        NObjectServer::EModificationType Type;

        void Persist(const NCellMaster::TPersistenceContext& context);
    };

    THashMap<
        NObjectServer::TRawObjectPtr<NTransactionServer::TTransaction>,
        std::vector<TPreparedNodeModification>
    > PreparedModifications_;

private:
    void SaveValues(NCellMaster::TSaveContext& context);
    void LoadValues(NCellMaster::TLoadContext& context);

    void Clear() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
