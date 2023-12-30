#pragma once

#include "public.h"
#include "master_table_schema.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NTableServer {

///////////////////////////////////////////////////////////////////////////////

struct ITableManager
    : public virtual TRefCounted
{
public:
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(MasterTableSchema, TMasterTableSchema);
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(TableCollocation, TTableCollocation);
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(SecondaryIndex, TSecondaryIndex);

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

    //! Creates an imported schema with the specified id.
    /*!
     *  An artificial ref is taken to ensure that native cell controls object's lifetime.
     *
     *  NB: This is the means of schema deduplication.
     */
    virtual TMasterTableSchema* CreateImportedMasterTableSchema(
        const NTableClient::TTableSchema& tableSchema,
        TMasterTableSchemaId hintId) = 0;

    //! Creates a foreign schema with the specified id.
    /*!
     *  Unlike the method above, it does not take a ref.
     *  Schema lifetime is controlled by the transaction schema is attached to.
     *
     *  NB: This is the means of schema deduplication.
     */
    virtual TMasterTableSchema* CreateImportedTemporaryMasterTableSchema(
        const NTableClient::TTableSchema& tableSchema,
        NTransactionServer::TTransaction* schemaHolder,
        TMasterTableSchemaId hintId) = 0;

    // COMPAT(h0pless): RefactorSchemaExport
    virtual TMasterTableSchema* CreateImportedMasterTableSchema(
        const NTableClient::TTableSchema& tableSchema,
        TSchemafulNode* schemaHolder,
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
        TSchemafulNode* schemaHolder) = 0;

    //! Same as above but associates resulting schema with a transaction instead
    //! of a schemaful node.
    virtual TMasterTableSchema* GetOrCreateNativeMasterTableSchema(
        const NTableClient::TTableSchema& schema,
        NTransactionServer::TTransaction* schemaHolder) = 0;

    //! Same as above but associates resulting schema with a chunk instead
    //! of a transaction.
    virtual TMasterTableSchema* GetOrCreateNativeMasterTableSchema(
        const NTableClient::TTableSchema& schema,
        NChunkServer::TChunk* schemaHolder) = 0;

    // For loading from snapshot.
    virtual TMasterTableSchema::TNativeTableSchemaToObjectMapIterator RegisterNativeSchema(
        TMasterTableSchema* schema,
        NTableClient::TTableSchema tableSchema) = 0;

    virtual TMasterTableSchema* GetEmptyMasterTableSchema() const = 0;

    virtual void SetTableSchema(TSchemafulNode* table, TMasterTableSchema* schema) = 0;
    virtual void SetTableSchemaOrThrow(TSchemafulNode* table, TMasterTableSchemaId schemaId) = 0;
    virtual void SetTableSchemaOrCrash(TSchemafulNode* table, TMasterTableSchemaId schemaId) = 0;
    virtual void ResetTableSchema(TSchemafulNode* table) = 0;

    // NB: Additional set methods are not needed for chunks, since chunks are immutable.
    // On top of that they use smart pointers over schemas, thus removing the need to use manual reset.
    virtual void SetChunkSchema(NChunkServer::TChunk* chunk, TMasterTableSchema* schema) = 0;

    //! Increases export ref counter of a schema.
    /*!
     *  Iff schema is not yet exported to #dstCellTag sends it.
    */
    virtual void ExportMasterTableSchema(
        TMasterTableSchema* schema,
        NObjectClient::TCellTag dstCellTag) = 0;

    //! Decreases export ref counter of a schema.
    /*!
     *  Iff it becomes 0 sends a mutation that destroys schema on cell #dstCellTag.
    */
    virtual void UnexportMasterTableSchema(
        TMasterTableSchema* schema,
        NObjectClient::TCellTag dstCellTag,
        int decreaseBy = 1) = 0;

    // COMPAT(h0pless): Remove isChunkSchema flag after chunk schemas are introduced.
    virtual void ValidateTableSchemaCorrespondence(
        NCypressClient::TVersionedNodeId nodeId,
        const NTableClient::TTableSchemaPtr& tableSchema,
        TMasterTableSchemaId schemaId,
        bool isChunkSchema = false) = 0;

    virtual const NTableClient::TTableSchema* ProcessSchemaFromAttributes(
        NTableClient::TTableSchemaPtr& tableSchema,
        TMasterTableSchemaId schemaId,
        bool dynamic,
        bool chaos,
        NCypressClient::TVersionedNodeId nodeId) = 0;

    //! Secondary index management.
    virtual TSecondaryIndex* CreateSecondaryIndex(
        NObjectClient::TObjectId hintId,
        ESecondaryIndexKind type,
        TTableNode* table,
        TTableNode* secondaryIndex) = 0;

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

    // COMPAT(h0pless): Remove this after schema migration is complete.
    virtual void TransformForeignSchemaIdsToNative() = 0;

    // COMPAT(h0pless): RecomputeMasterTableSchemaRefCounters
    virtual void RecomputeMasterTableSchemaExportRefCounters(NLogging::ELogLevel logLevel) = 0;
    virtual void RecomputeMasterTableSchemaRefCounters(NLogging::ELogLevel logLevel) = 0;

    virtual void OnTableCopied(TTableNode* sourceNode, TTableNode* clonedNode) = 0;

    DECLARE_INTERFACE_SIGNAL(void(NTabletServer::TTableCollocationData), ReplicationCollocationCreated);
    DECLARE_INTERFACE_SIGNAL(void(NTableClient::TTableCollocationId), ReplicationCollocationDestroyed);
};

DEFINE_REFCOUNTED_TYPE(ITableManager)

////////////////////////////////////////////////////////////////////////////////

ITableManagerPtr CreateTableManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
