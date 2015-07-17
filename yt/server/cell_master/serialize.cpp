#include "stdafx.h"
#include "serialize.h"
#include "bootstrap.h"

#include <ytlib/object_client/helpers.h>

#include <server/node_tracker_server/node_tracker.h>

#include <server/transaction_server/transaction_manager.h>

#include <server/object_server/object_detail.h>

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_owner_base.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/job.h>
#include <server/chunk_server/chunk_manager.h>

#include <server/cypress_server/cypress_manager.h>

#include <server/security_server/security_manager.h>
#include <server/security_server/user.h>
#include <server/security_server/group.h>

#include <server/table_server/table_node.h>

#include <server/tablet_server/tablet_manager.h>
#include <server/tablet_server/tablet_cell.h>

namespace NYT {
namespace NCellMaster {

using namespace NNodeTrackerServer;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NTransactionServer;
using namespace NChunkServer;
using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NTabletServer;
using namespace NTableServer;

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 119 ||
        version == 200;
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
