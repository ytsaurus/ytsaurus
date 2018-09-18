#include "system_tables.h"

#include "storage_system_cluster.h"

#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>

#include <Storages/System/attachSystemTables.h>

#include <Storages/System/StorageSystemAsynchronousMetrics.h>
#include <Storages/System/StorageSystemBuildOptions.h>
#include <Storages/System/StorageSystemClusters.h>
#include <Storages/System/StorageSystemColumns.h>
#include <Storages/System/StorageSystemDatabases.h>
#include <Storages/System/StorageSystemDictionaries.h>
#include <Storages/System/StorageSystemEvents.h>
#include <Storages/System/StorageSystemFunctions.h>
#include <Storages/System/StorageSystemGraphite.h>
#include <Storages/System/StorageSystemMerges.h>
#include <Storages/System/StorageSystemMetrics.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Storages/System/StorageSystemOne.h>
#include <Storages/System/StorageSystemParts.h>
#include <Storages/System/StorageSystemProcesses.h>
#include <Storages/System/StorageSystemReplicas.h>
#include <Storages/System/StorageSystemReplicationQueue.h>
#include <Storages/System/StorageSystemSettings.h>
#include <Storages/System/StorageSystemTables.h>
#include <Storages/System/StorageSystemZooKeeper.h>

namespace NYT {
namespace NClickHouse {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

// See dbms/src/Storages/System/attachSystemTables.cpp

void AttachSystemTablesServer(IDatabase& system)
{
    // Attach only relevant system tables

    system.attachTable("processes", StorageSystemProcesses::create("processes"));
    system.attachTable("metrics", StorageSystemMetrics::create("metrics"));
    system.attachTable("merges", StorageSystemMerges::create("merges"));
    system.attachTable("dictionaries", StorageSystemDictionaries::create("dictionaries"));
}

void AttachRelevantClickHouseSystemTables(IDatabase& system)
{
    attachSystemTablesLocal(system);
    AttachSystemTablesServer(system);
}

void AttachClusterSystemTable(IDatabase& system, IClusterNodeTrackerPtr clusterTracker)
{
    system.attachTable("cluster", CreateStorageSystemCluster(clusterTracker, "cluster"));
}

void AttachSystemTables(IDatabase& system, IClusterNodeTrackerPtr clusterTracker)
{
    AttachRelevantClickHouseSystemTables(system);
    AttachClusterSystemTable(system, clusterTracker);
}

}   // namespace NClickHouse
}   // namespace NYT
