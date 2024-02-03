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
    TEntitySerializationKey NextSerializationKey_;
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

class TBeginCopyContext
    : public TEntityStreamSaveContext
{
public:
    TBeginCopyContext(
        NTransactionServer::TTransaction* transaction,
        ENodeCloneMode mode,
        const TCypressNode* rootNode);

    void RegisterPortalRootId(TNodeId portalRootId);
    void RegisterOpaqueChildPath(const NYPath::TYPath& opaqueChildPath);
    void RegisterExternalCellTag(NObjectClient::TCellTag cellTag);
    TEntitySerializationKey RegisterSchema(NTableServer::TMasterTableSchema* schema);
    const THashMap<NTableServer::TMasterTableSchema*, TEntitySerializationKey>& GetRegisteredSchemas() const;

    DEFINE_BYREF_RO_PROPERTY(std::vector<TNodeId>, PortalRootIds);
    DEFINE_BYREF_RO_PROPERTY(std::vector<NYPath::TYPath>, OpaqueChildPaths);
    DEFINE_BYVAL_RO_PROPERTY(NTransactionServer::TTransaction*, Transaction);
    DEFINE_BYVAL_RO_PROPERTY(ENodeCloneMode, Mode);
    DEFINE_BYVAL_RO_PROPERTY(const TCypressNode*, RootNode);

    std::vector<TSharedRef> Finish();
    NObjectClient::TCellTagList GetExternalCellTags();

private:
    TCellLocalObjectSaveRegistry<NTableServer::TMasterTableSchema> SchemaRegistry_;
    TChunkedOutputStream Stream_;
    std::vector<NObjectClient::TCellTag> ExternalCellTags_;
};

////////////////////////////////////////////////////////////////////////////////

class TEndCopyContext
    : public TEntityStreamLoadContext
{
public:
    TEndCopyContext(
        NCellMaster::TBootstrap* bootstrap,
        ENodeCloneMode mode,
        TRef data);

    template <class T>
    T* GetObject(NObjectServer::TObjectId id);

    template <class T>
    const TInternRegistryPtr<T>& GetInternRegistry() const;

    DEFINE_BYVAL_RO_PROPERTY(ENodeCloneMode, Mode);

    void RegisterSchema(TEntitySerializationKey key, NTableServer::TMasterTableSchema* schema);
    NTableServer::TMasterTableSchema* GetSchemaOrThrow(TEntitySerializationKey key);

    bool IsOpaqueChild() const;
    void SetOpaqueChild(bool opaqueChild);

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    TCellLocalObjectLoadRegistry<NTableServer::TMasterTableSchema> SchemaRegistry_;
    TMemoryInput Stream_;
    bool OpaqueChild_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_
