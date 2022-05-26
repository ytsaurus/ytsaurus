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

    //! Looks up a master table schema by id. Throws if no such schema exists.
    virtual TMasterTableSchema* GetMasterTableSchemaOrThrow(TMasterTableSchemaId id) = 0;

    //! Looks up a table schema. Returns an existing schema object or nullptr if
    //! no such schema exists. This is the means of schema deduplication.
    virtual TMasterTableSchema* FindMasterTableSchema(const NTableClient::TTableSchema& schema) const = 0;

    //! Looks up a schema or creates one if no such schema exists.
    /*!
     *  #schemaHolder will have its schema set to the resulting schema.
     *  The schema itself will be referenced by the table.
     *
     *  NB: This is the means of schema deduplication.
     */
    virtual TMasterTableSchema* GetOrCreateMasterTableSchema(
        const NTableClient::TTableSchema& schema,
        ISchemafulNode* schemaHolder) = 0;

    //! Same as above but associates resulting schema with a transaction instead
    //! of a table.
    virtual TMasterTableSchema* GetOrCreateMasterTableSchema(
        const NTableClient::TTableSchema& schema,
        NTransactionServer::TTransaction* schemaHolder) = 0;

    //! Creates a new schema object with a specified ID.
    //! The object will be free-floating and will have zero refcounter.
    // COMPAT(shakurov)
    virtual TMasterTableSchema* CreateMasterTableSchemaUnsafely(
        TMasterTableSchemaId schemaId,
        const NTableClient::TTableSchema& schema) = 0;

    // For loading from snapshot.
    virtual TMasterTableSchema::TTableSchemaToObjectMapIterator RegisterSchema(
        TMasterTableSchema* schema,
        NTableClient::TTableSchema tableSchema) = 0;

    virtual TMasterTableSchema* GetEmptyMasterTableSchema() = 0;

    // COMPAT(shakurov)
    virtual TMasterTableSchema* GetOrCreateEmptyMasterTableSchema() = 0;

    virtual void SetTableSchema(ISchemafulNode* table, TMasterTableSchema* schema) = 0;
    virtual void ResetTableSchema(ISchemafulNode* table) = 0;

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
};

DEFINE_REFCOUNTED_TYPE(ITableManager)

////////////////////////////////////////////////////////////////////////////////

ITableManagerPtr CreateTableManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
