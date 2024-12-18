#include "serialize.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/server/master/table_server/master_table_schema.h>
#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/master/tablet_server/tablet_manager.h>

#include <yt/yt/server/master/chaos_server/chaos_manager.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NSecurityServer;
using namespace NObjectServer;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NTabletServer;
using namespace NChaosServer;
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
    : TEntityStreamSaveContext(
        &Stream_,
        NCellMaster::GetCurrentReign())
    , Transaction_(transaction)
    , Mode_(mode)
    , RootNode_(rootNode)
{ }

std::vector<TSharedRef> TBeginCopyContext::Finish()
{
    TStreamSaveContext::Finish();
    return Stream_.Finish();
}

void TBeginCopyContext::RegisterSchema(TMasterTableSchemaId schemaId)
{
    YT_ASSERT(!SchemaId_);
    SchemaId_ = schemaId;
}

NTableServer::TMasterTableSchemaId TBeginCopyContext::GetSchemaId() const
{
    return SchemaId_;
}

////////////////////////////////////////////////////////////////////////////////

TEndCopyContext::TEndCopyContext(
    TBootstrap* bootstrap,
    ENodeCloneMode mode,
    TRef data,
    NTableServer::TMasterTableSchemaId schemaId,
    TNodeId inplaceLoadTargetNodeId)
    : TEntityStreamLoadContext(&Stream_)
    , Mode_(mode)
    , InplaceLoadTargetNodeId_(inplaceLoadTargetNodeId)
    , Bootstrap_(bootstrap)
    , Stream_(data.Begin(), data.Size())
    , SchemaId_(schemaId)
{ }

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
    return Bootstrap_->GetTabletManager()->GetTabletCellBundleOrThrow(id, true);
}

template <>
TChaosCellBundle* TEndCopyContext::GetObject(TObjectId id)
{
    return Bootstrap_->GetChaosManager()->GetChaosCellBundleOrThrow(id, true);
}

template <>
const TSecurityTagsRegistryPtr& TEndCopyContext::GetInternRegistry() const
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    return securityManager->GetSecurityTagsRegistry();
}

TMasterTableSchema* TEndCopyContext::GetSchema() const
{
    const auto& tableManager = Bootstrap_->GetTableManager();
    return tableManager->GetMasterTableSchema(SchemaId_);
}

void TEndCopyContext::RegisterChild(const std::string& key, TNodeId childId)
{
    Children_.emplace_back(key, childId);
}

bool TEndCopyContext::HasChildren() const
{
    return !Children_.empty();
}

std::vector<std::pair<std::string, TNodeId>> TEndCopyContext::GetChildren() const
{
    return Children_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
