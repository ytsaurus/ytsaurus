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

// TODO(h0pless): IntroduceNewPipelineForCrossCellCopy.
// Rename this to SerializeNodeContext in a separate PR. Just to make sure that diff is more readable.
class TSerializeNodeContext
    : public TEntityStreamSaveContext
{
public:
    TSerializeNodeContext(
        NTransactionServer::TTransaction* transaction,
        ENodeCloneMode mode,
        const TCypressNode* rootNode);

    void RegisterSchema(NTableServer::TMasterTableSchemaId schemaId);
    NTableServer::TMasterTableSchemaId GetSchemaId() const;

    std::vector<TSharedRef> Finish();

    DEFINE_BYVAL_RO_PROPERTY(NTransactionServer::TTransaction*, Transaction);
    DEFINE_BYVAL_RO_PROPERTY(ENodeCloneMode, Mode);
    DEFINE_BYVAL_RO_PROPERTY(const TCypressNode*, RootNode);
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TCellTag, ExternalCellTag, NObjectClient::NotReplicatedCellTagSentinel);

private:
    TChunkedOutputStream Stream_;

    NTableServer::TMasterTableSchemaId SchemaId_ = NObjectServer::NullObjectId;
    std::vector<TCypressNode*> Children_;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(h0pless): IntroduceNewPipelineForCrossCellCopy.
// Rename this to MaterializeNodeContext in a separate PR. Just to make sure that diff is more readable.
class TMaterializeNodeContext
    : public TEntityStreamLoadContext
{
public:
    TMaterializeNodeContext(
        NCellMaster::TBootstrap* bootstrap,
        ENodeCloneMode mode,
        TRef data,
        NTableServer::TMasterTableSchemaId schemaId = NObjectServer::NullObjectId,
        TNodeId inplaceLoadTargetNodeId = NCypressClient::NullObjectId);

    template <class T>
    T* GetObject(NObjectServer::TObjectId id);

    template <class T>
    const TInternRegistryPtr<T>& GetInternRegistry() const;

    DEFINE_BYVAL_RO_PROPERTY(ENodeCloneMode, Mode);
    DEFINE_BYVAL_RO_PROPERTY(TNodeId, InplaceLoadTargetNodeId);
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TCellTag, ExternalCellTag);

    NTableServer::TMasterTableSchema* GetSchema() const;

    void RegisterChild(const std::string& key, TNodeId childId);
    bool HasChildren() const;
    std::vector<std::pair<std::string, TNodeId>> GetChildren() const;

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    TMemoryInput Stream_;

    std::vector<std::pair<std::string, TNodeId>> Children_;

    NTableServer::TMasterTableSchemaId SchemaId_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_
