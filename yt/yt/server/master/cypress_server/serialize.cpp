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

static const auto& Logger = CypressServerLogger();

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

void TBeginCopyContext::RegisterExternalCellTag(TCellTag cellTag)
{
    YT_ASSERT(!ExternalCellTag_);
    ExternalCellTag_ = cellTag;
}

void TBeginCopyContext::RegisterPortalRoot(TNodeId portalRootId)
{
    PortalRootId_ = portalRootId;
}

void TBeginCopyContext::RegisterAsOpaque(const NYPath::TYPath& path)
{
    YT_ASSERT(!Path_ && !path.empty());
    Path_ = path;
}

void TBeginCopyContext::RegisterChild(TCypressNode* child)
{
    Children_.push_back(child);
}

std::optional<NTableServer::TMasterTableSchemaId> TBeginCopyContext::GetSchemaId() const
{
    return SchemaId_;
}

std::optional<NObjectClient::TCellTag> TBeginCopyContext::GetExternalCellTag() const
{
    return ExternalCellTag_;
}

std::optional<NCypressClient::TNodeId> TBeginCopyContext::GetPortalRootId() const
{
    return PortalRootId_;
}

std::optional<NYPath::TYPath> TBeginCopyContext::GetPathIfOpaque() const
{
    return Path_;
}

const std::vector<TCypressNode*>& TBeginCopyContext::GetChildren() const
{
    return Children_;
}

////////////////////////////////////////////////////////////////////////////////

TEndCopyContext::TEndCopyContext(
    TBootstrap* bootstrap,
    ENodeCloneMode mode,
    TRef data,
    const THashMap<NTableServer::TMasterTableSchemaId, NTableServer::TMasterTableSchema*>& schemaIdToSchema)
    : TEntityStreamLoadContext(&Stream_)
    , Mode_(mode)
    , Bootstrap_(bootstrap)
    , SchemaIdToSchema_(schemaIdToSchema)
    , Stream_(data.Begin(), data.Size())
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
    return Bootstrap_->GetTabletManager()->GetTabletCellBundleOrThrow(id);
}

template <>
TChaosCellBundle* TEndCopyContext::GetObject(TObjectId id)
{
    return Bootstrap_->GetChaosManager()->GetChaosCellBundleOrThrow(id);
}

template <>
const TSecurityTagsRegistryPtr& TEndCopyContext::GetInternRegistry() const
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    return securityManager->GetSecurityTagsRegistry();
}

TMasterTableSchema* TEndCopyContext::GetSchema(TMasterTableSchemaId schemaId) const
{
    return GetOrCrash(SchemaIdToSchema_, schemaId);
}

void TEndCopyContext::RegisterChild(TString key, TNodeId childId)
{
    Children_.push_back(std::pair(key, childId));
}

bool TEndCopyContext::HasChildren() const
{
    return !Children_.empty();
}

std::vector<std::pair<TString, TNodeId>> TEndCopyContext::GetChildren() const
{
    return Children_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
