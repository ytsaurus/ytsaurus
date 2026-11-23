#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/automaton.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/core/phoenix/type_decl.h>

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

        using TSaveContext = NCellMaster::TSaveContext;
        using TLoadContext = NCellMaster::TLoadContext;

        PHOENIX_DECLARE_TYPE(TPreparedNodeModification, 0xe8e3c29f);
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

} // namespace NYT::NCypressServer
