#pragma once

#include "public.h"

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/tablet_server/public.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/memory/chunked_output_stream.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

// Used for cross-cell copying of cell-local objects (e.g. schemas).
template <class T>
class TCellLocalObjectSaveRegistry
{
public:
    TEntitySerializationKey RegisterObject(T* object);
    const THashMap<T*, TEntitySerializationKey>& RegisteredObjects() const;

private:
    THashMap<T*, TEntitySerializationKey> RegisteredObjects_;
    TEntitySerializationKey NextSerializationKey_{0};
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TCellLocalObjectLoadRegistry
{
public:
    void RegisterObject(TEntitySerializationKey key, T* object);
    T* GetObjectOrThrow(TEntitySerializationKey key);

private:
    THashMap<TEntitySerializationKey, T*> RegisteredObjects_;
};

////////////////////////////////////////////////////////////////////////////////

// Rename this to TBeginCopyNodeContext
class TBeginCopyContext
    : public TEntityStreamSaveContext
{
public:
    TBeginCopyContext(
        NTransactionServer::TTransaction* transaction,
        ENodeCloneMode mode,
        const TCypressNode* rootNode);

    void RegisterSchema(NTableServer::TMasterTableSchemaId schemaId);
    void RegisterExternalCellTag(NObjectClient::TCellTag cellTag);
    void RegisterPortalRoot(NCypressClient::TNodeId portalRootId);
    void RegisterAsOpaque(const NYPath::TYPath& path);
    void RegisterChild(TCypressNode* child);

    std::optional<NTableServer::TMasterTableSchemaId> GetSchemaId() const;
    std::optional<NObjectClient::TCellTag> GetExternalCellTag() const;
    std::optional<NCypressClient::TNodeId> GetPortalRootId() const;
    std::optional<NYPath::TYPath> GetPathIfOpaque() const;
    const std::vector<TCypressNode*>& GetChildren() const;

    std::vector<TSharedRef> Finish();

    DEFINE_BYVAL_RO_PROPERTY(NTransactionServer::TTransaction*, Transaction);
    DEFINE_BYVAL_RO_PROPERTY(ENodeCloneMode, Mode);
    DEFINE_BYVAL_RO_PROPERTY(const TCypressNode*, RootNode);

private:
    TChunkedOutputStream Stream_;

    std::optional<NTableServer::TMasterTableSchemaId> SchemaId_;
    std::optional<NObjectClient::TCellTag> ExternalCellTag_;
    std::optional<NCypressClient::TNodeId> PortalRootId_;
    // Used iff node is marked as opaque.
    std::optional<NYPath::TYPath> Path_;
    std::vector<TCypressNode*> Children_;
};

////////////////////////////////////////////////////////////////////////////////

class TEndCopyContext
    : public TEntityStreamLoadContext
{
public:
    TEndCopyContext(
        NCellMaster::TBootstrap* bootstrap,
        ENodeCloneMode mode,
        TRef data,
        const THashMap<NTableServer::TMasterTableSchemaId, NTableServer::TMasterTableSchema*>& schemaIdToSchema);

    template <class T>
    T* GetObject(NObjectServer::TObjectId id);

    template <class T>
    const TInternRegistryPtr<T>& GetInternRegistry() const;

    DEFINE_BYVAL_RO_PROPERTY(ENodeCloneMode, Mode);
    DEFINE_BYVAL_RW_BOOLEAN_PROPERTY(OpaqueChild);

    NTableServer::TMasterTableSchema* GetSchema(NTableServer::TMasterTableSchemaId schemaId) const;

    void RegisterChild(TString key, TNodeId childId);
    bool HasChildren() const;
    std::vector<std::pair<TString, TNodeId>> GetChildren() const;

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    const THashMap<NTableServer::TMasterTableSchemaId, NTableServer::TMasterTableSchema*>& SchemaIdToSchema_;
    TMemoryInput Stream_;

    std::vector<std::pair<TString, TNodeId>> Children_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_
