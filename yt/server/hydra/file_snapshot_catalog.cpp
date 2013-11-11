#include "stdafx.h"
#include "file_snapshot_catalog.h"
#include "file_snapshot.h"
#include "snapshot.h"
#include "snapshot_catalog.h"
#include "config.h"
#include "private.h"

#include <core/misc/fs.h>

#include <util/folder/dirut.h>

namespace NYT {
namespace NHydra {

using namespace NFS;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = HydraLogger;

////////////////////////////////////////////////////////////////////////////////

class TFileSnapshotCatalog
    : public ISnapshotCatalog
{
public:
    explicit TFileSnapshotCatalog(TFileSnapshotCatalogConfigPtr config)
        : Config(config)
    { }

    void Start()
    {
        LOG_INFO("Starting snapshot catalog");

        try {
            // Initialize root directory.
            {
                ForcePath(Config->Path);
            }

            // Initialize stores.
            {
                auto entries = EnumerateDirectories(Config->Path);
                for (const auto& entry : entries) {
                    TCellGuid cellGuid;
                    auto cellGuidStr = GetFileNameWithoutExtension(entry);
                    if (!TCellGuid::FromString(cellGuidStr, &cellGuid)) {
                        LOG_FATAL("Error parsing cell GUID %s", ~cellGuidStr.Quote());
                    }

                    LOG_INFO("Found snapshot store %s", ~ToString(cellGuid));

                    DoCreateStore(cellGuid);
                }
            }
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error starting snapshot catalog");
        }

        LOG_INFO("Snapshot catalog started");
    }

    virtual std::vector<ISnapshotStorePtr> GetStores() override
    {
        TGuard<TSpinLock> guard(SpinLock);
        std::vector<ISnapshotStorePtr> result;
        for (const auto& pair : StoreMap) {
            result.push_back(pair.second);
        }
        return result;
    }

    virtual ISnapshotStorePtr CreateStore(const TCellGuid& cellGuid) override
    {
        try {
            auto path = GetStorePath(cellGuid);
            ForcePath(path);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error creating snapshot store %s",
                ~ToString(cellGuid));
        }

        auto store = DoCreateStore(cellGuid);

        LOG_INFO("Created snapshot store %s", ~ToString(cellGuid));

        return store;
    }

    virtual void RemoveStore(const TCellGuid& cellGuid) override
    {
        {
            TGuard<TSpinLock> guard(SpinLock);
            YCHECK(StoreMap.erase(cellGuid) == 1);
        }

        try {
            auto path = GetStorePath(cellGuid);
            RemoveDirWithContents(path);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error removing snapshot store %s",
                ~ToString(cellGuid));
        }

        LOG_INFO("Removed snapshot store %s", ~ToString(cellGuid));
    }

    virtual ISnapshotStorePtr FindStore(const TCellGuid& cellGuid) override
    {
        TGuard<TSpinLock> guard(SpinLock);
        auto it = StoreMap.find(cellGuid);
        return it == StoreMap.end() ? nullptr : it->second;
    }

private:
    TFileSnapshotCatalogConfigPtr Config;

    //! Protects a section of members.
    TSpinLock SpinLock;
    yhash_map<TCellGuid, ISnapshotStorePtr> StoreMap;


    Stroka GetStorePath(const TCellGuid& cellGuid)
    {
        return CombinePaths(Config->Path, ToString(cellGuid));
    }

    ISnapshotStorePtr DoCreateStore(const TCellGuid& cellGuid)
    {
        auto storeConfig = New<TFileSnapshotStoreConfig>();
        storeConfig->Path = GetStorePath(cellGuid);
        storeConfig->Codec = Config->Codec;

        auto store = CreateFileSnapshotStore(cellGuid, storeConfig);

        {
            TGuard<TSpinLock> guard(SpinLock);
            YCHECK(StoreMap.insert(std::make_pair(cellGuid, store)).second);
        }

        return store;
    }

};

ISnapshotCatalogPtr CreateFileSnapshotCatalog(
    TFileSnapshotCatalogConfigPtr config)
{
    auto catalog = New<TFileSnapshotCatalog>(config);
    catalog->Start();
    return catalog;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

