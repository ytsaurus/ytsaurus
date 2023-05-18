#pragma once

#include "public.h"
#include "master_table_schema.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/lib/hydra_common/entity_map.h>

#include <yt/yt/core/misc/ref_counted.h>

namespace NYT::NTableServer {

///////////////////////////////////////////////////////////////////////////////

struct ITableManager
    : public virtual TRefCounted
{
public:
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(MasterTableSchema, TMasterTableSchema);
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(TableCollocation, TTableCollocation);

    virtual void Initialize() = 0;

    virtual void ScheduleStatisticsUpdate(
        NChunkServer::TChunkOwnerBase* chunkOwner,
        bool updateDataStatistics = true,
        bool updateTabletStatistics = true,
        bool useNativeContentRevisionCas = false) = 0;

    virtual void SendStatisticsUpdate(
        NChunkServer::TChunkOwnerBase* chunkOwner,
        bool useNativeContentRevisionCas = false) = 0;

    //! Looks up a table by id. Throws if no such table exists.
    virtual TTableNode* GetTableNodeOrThrow(TTableId id) = 0;

    //! Looks up a table by id. Returns null if no such table exists.
    virtual TTableNode* FindTableNode(TTableId id) = 0;

    //! Looks up a master table schema by id. Throws if no such schema exists.
    virtual TMasterTableSchema* GetMasterTableSchemaOrThrow(TMasterTableSchemaId id) = 0;

    //! Looks up a table schema. Returns an existing schema object or nullptr if no such schema exists.
    //! This is the means of schema deduplication.
    virtual TMasterTableSchema* FindNativeMasterTableSchema(
        const NTableClient::TTableSchema& tableSchema) const = 0;

    //! Looks up a table schema and cellTag combination. Returns an existing schema object or nullptr if
    //! no such pair exists. This is the means of schema deduplication.
    virtual TMasterTableSchema* FindImportedMasterTableSchema(
        const NTableClient::TTableSchema& tableSchema,
        const NObjectClient::TCellTag cellTag) const = 0;

    //! Creates an imported schema with the specified id. If id is null - generates a new id.
    /*!
     *  #schemaHolder will have its schema set to the resulting schema.
     *  The schema itself will be referenced by the table.
     *
     *  NB: This is the means of schema deduplication.
     */
    virtual TMasterTableSchema* CreateImportedMasterTableSchema(
        const NTableClient::TTableSchema& tableSchema,
        ISchemafulNode* schemaHolder,
        TMasterTableSchemaId hintId) = 0;

    //! Same as above but associates resulting schema with a transaction instead
    //! of a table.
    virtual TMasterTableSchema* CreateImportedMasterTableSchema(
        const NTableClient::TTableSchema& tableSchema,
        NTransactionServer::TTransaction* schemaHolder,
        TMasterTableSchemaId hintId) = 0;

    //! Looks up a schema or creates one if no such schema exists.
    /*!
     *  #schemaHolder will have its schema set to the resulting schema.
     *  The schema itself will be referenced by the schemaful node.
     *
     *  NB: This is the means of schema deduplication.
     */
    virtual TMasterTableSchema* GetOrCreateNativeMasterTableSchema(
        const NTableClient::TTableSchema& schema,
        ISchemafulNode* schemaHolder) = 0;

    //! Same as above but associates resulting schema with a transaction instead
    //! of a schemaful node.
    virtual TMasterTableSchema* GetOrCreateNativeMasterTableSchema(
        const NTableClient::TTableSchema& schema,
        NTransactionServer::TTransaction* schemaHolder) = 0;

    // For loading from snapshot.
    virtual TMasterTableSchema::TNativeTableSchemaToObjectMapIterator RegisterNativeSchema(
        TMasterTableSchema* schema,
        NTableClient::TTableSchema tableSchema) = 0;
    virtual TMasterTableSchema::TImportedTableSchemaToObjectMapIterator RegisterImportedSchema(
        TMasterTableSchema* schema,
        NTableClient::TTableSchema tableSchema) = 0;

    virtual TMasterTableSchema* GetEmptyMasterTableSchema() const = 0;

    virtual void SetTableSchema(ISchemafulNode* table, TMasterTableSchema* schema) = 0;
    virtual void SetTableSchemaOrThrow(ISchemafulNode* table, TMasterTableSchemaId schemaId) = 0;
    virtual void SetTableSchemaOrCrash(ISchemafulNode* table, TMasterTableSchemaId schemaId) = 0;
    virtual void ResetTableSchema(ISchemafulNode* table) = 0;

    virtual void ValidateTableSchemaCorrespondence(
        NCypressClient::TVersionedNodeId nodeId,
        const NTableClient::TTableSchemaPtr& tableSchema,
        TMasterTableSchemaId schemaId) = 0;

    virtual const NTableClient::TTableSchema* ProcessSchemaFromAttributes(
        NTableClient::TTableSchemaPtr& tableSchema,
        TMasterTableSchemaId schemaId,
        bool dynamic,
        bool chaos,
        NCypressClient::TVersionedNodeId nodeId) = 0;

    //! Schema export counter to cell tag is incremented by 1.
    //! Schema is also sent to the external cell iff it was not exported there before.
    virtual void EnsureSchemaExported(
        const ISchemafulNode* node,
        NObjectClient::TCellTag externalCellTag,
        NTransactionServer::TTransactionId externalizedTransactionId,
        const NCellMaster::IMulticellManagerPtr& multicellManager) = 0;

    //! Table collocation management.
    virtual TTableCollocation* CreateTableCollocation(
        NObjectClient::TObjectId hintId,
        ETableCollocationType type,
        THashSet<TTableNode*> collocatedTables) = 0;
    virtual void ZombifyTableCollocation(TTableCollocation* collocation) = 0;
    virtual void AddTableToCollocation(TTableNode* table, TTableCollocation* collocation) = 0;
    virtual void RemoveTableFromCollocation(TTableNode* table, TTableCollocation* collocation) = 0;
    virtual TTableCollocation* GetTableCollocationOrThrow(TTableCollocationId id) const = 0;

    // Queue agent object management.

    virtual const THashSet<TTableNode*>& GetQueues() const = 0;
    virtual void RegisterQueue(TTableNode* node) = 0;
    virtual void UnregisterQueue(TTableNode* node) = 0;

    virtual const THashSet<TTableNode*>& GetConsumers() const = 0;
    virtual void RegisterConsumer(TTableNode* node) = 0;
    virtual void UnregisterConsumer(TTableNode* node) = 0;

    virtual TFuture<NYson::TYsonString> GetQueueAgentObjectRevisionsAsync() const = 0;

    DECLARE_INTERFACE_SIGNAL(void(NTabletServer::TTableCollocationData), ReplicationCollocationUpdated);
    DECLARE_INTERFACE_SIGNAL(void(NTableClient::TTableCollocationId), ReplicationCollocationDestroyed);
};

DEFINE_REFCOUNTED_TYPE(ITableManager)

////////////////////////////////////////////////////////////////////////////////

ITableManagerPtr CreateTableManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
