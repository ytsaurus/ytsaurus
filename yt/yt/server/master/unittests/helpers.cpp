#include "helpers.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/object_server/object.h>
#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/actions/invoker_util.h>

namespace NYT {

using namespace NConcurrency;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TBootstrapMock::TBootstrapMock()
{
    HydraFacade_ = CreateHydraFacade(TTestingTag());
    ObjectManager_ = CreateObjectManager(TTestingTag(), this);
}

void TBootstrapMock::SetupMasterSmartpointers()
{
    SetupMasterBootstrap(this);
    SetupAutomatonThread();

    auto epochContext = New<TEpochContext>();
    epochContext->CurrentEpoch = 42;
    epochContext->CurrentEpochCounter = 42;
    epochContext->EphemeralPtrUnrefInvoker = GetSyncInvoker();
    SetupEpochContext(epochContext);

    BeginMutation();
}

void TBootstrapMock::ResetMasterSmartpointers()
{
    ResetAll();
}

////////////////////////////////////////////////////////////////////////////////

TGuid GenerateId(EObjectType type)
{
    static i64 counter = 0;
    return MakeId(type, TCellTag(0), counter++, 0);
}

#define XX(object)\
TGuid Generate##object##Id() \
{ \
    return GenerateId(EObjectType::object); \
} \

XX(Chunk)
XX(ChunkList)
XX(TabletCell)
XX(TabletCellBundle)
XX(ClusterNode)

#undef XX

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
