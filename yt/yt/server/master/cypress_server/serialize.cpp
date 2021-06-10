#include "serialize.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/server/master/table_server/master_table_schema.h>
#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/master/tablet_server/tablet_manager.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NSecurityServer;
using namespace NObjectServer;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NTabletServer;
using namespace NTableClient;
using namespace NTableServer;
using namespace NTransactionServer;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TBeginCopyContext::TBeginCopyContext(
    TTransaction* transaction,
    ENodeCloneMode mode,
    const TCypressNode* rootNode)
    : Transaction_(transaction)
    , Mode_(mode)
    , RootNode_(rootNode)
{
    SetOutput(&Stream_);
}

std::vector<TSharedRef> TBeginCopyContext::Finish()
{
    return Stream_.Flush();
}

TCellTagList TBeginCopyContext::GetExternalCellTags()
{
    SortUnique(ExternalCellTags_);
    return TCellTagList(ExternalCellTags_.begin(), ExternalCellTags_.end());
}

void TBeginCopyContext::RegisterPortalRootId(TNodeId portalRootId)
{
    PortalRootIds_.push_back(portalRootId);
}

void TBeginCopyContext::RegisterOpaqueChildPath(const NYPath::TYPath& opaqueChildPath)
{
    OpaqueChildPaths_.push_back(opaqueChildPath);
}

void TBeginCopyContext::RegisterExternalCellTag(TCellTag cellTag)
{
    ExternalCellTags_.push_back(cellTag);
}

TEntitySerializationKey TBeginCopyContext::RegisterSchema(TMasterTableSchema* schema)
{
    YT_VERIFY(IsObjectAlive(schema));
    return SchemaRegistry_.RegisterObject(schema);
}

const THashMap<TMasterTableSchema*, TEntitySerializationKey>& TBeginCopyContext::GetRegisteredSchemas() const
{
    return SchemaRegistry_.RegisteredObjects();
}

////////////////////////////////////////////////////////////////////////////////

TEndCopyContext::TEndCopyContext(
    TBootstrap* bootstrap,
    ENodeCloneMode mode,
    TRef data)
    : Mode_(mode)
    , Bootstrap_(bootstrap)
    , Stream_(data.Begin(), data.Size())
{
    SetInput(&Stream_);
}

template <>
TSubject* TEndCopyContext::GetObject(TObjectId id)
{
    return Bootstrap_->GetSecurityManager()->GetSubjectOrThrow(id);
}

template <>
TAccount* TEndCopyContext::GetObject(TObjectId id)
{
    return Bootstrap_->GetSecurityManager()->GetAccountOrThrow(id);
}

template <>
TMedium* TEndCopyContext::GetObject(TObjectId id)
{
    return Bootstrap_->GetChunkManager()->GetMediumOrThrow(id);
}

template <>
TTabletCellBundle* TEndCopyContext::GetObject(TObjectId id)
{
    return Bootstrap_->GetTabletManager()->GetTabletCellBundleOrThrow(id);
}

template <>
const TSecurityTagsRegistryPtr& TEndCopyContext::GetInternRegistry() const
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    return securityManager->GetSecurityTagsRegistry();
}

void TEndCopyContext::RegisterSchema(TEntitySerializationKey key, TMasterTableSchema* schema)
{
    SchemaRegistry_.RegisterObject(key, schema);
}

TMasterTableSchema* TEndCopyContext::GetSchemaOrThrow(TEntitySerializationKey key)
{
    return SchemaRegistry_.GetObjectOrThrow(key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
