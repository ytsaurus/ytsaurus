#include "disk_space_sensor_producer.h"

#include "config.h"
#include "private.h"

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NCellMaster {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CellMasterLogger;

////////////////////////////////////////////////////////////////////////////////

class TDiskSpaceSensorProducer
    : public NProfiling::ISensorProducer
{
public:
    explicit TDiskSpaceSensorProducer(TCellMasterBootstrapConfigPtr config)
        : Config_(std::move(config))
    { }

    void CollectSensors(ISensorWriter* writer) override
    {
        try {
            auto snapshotsStorageDiskSpaceStatistics = NFS::GetDiskSpaceStatistics(Config_->Snapshots->Path);
            writer->AddGauge("/snapshots/free_space", snapshotsStorageDiskSpaceStatistics.FreeSpace);
            writer->AddGauge("/snapshots/available_space", snapshotsStorageDiskSpaceStatistics.AvailableSpace);
        } catch (const std::exception& ex) {
            YT_TLOG_DEBUG("Failed to profile snapshots storage disk space")
                .With(ex);
        }

        try {
            auto changelogsStorageDiskSpaceStatistics = NFS::GetDiskSpaceStatistics(Config_->Changelogs->Path);
            writer->AddGauge("/changelogs/free_space", changelogsStorageDiskSpaceStatistics.FreeSpace);
            writer->AddGauge("/changelogs/available_space", changelogsStorageDiskSpaceStatistics.AvailableSpace);
        } catch (const std::exception& ex) {
            YT_TLOG_DEBUG("Failed to profile changelogs storage disk space")
                .With(ex);
        }
    }

private:
    const TCellMasterBootstrapConfigPtr Config_;
};

////////////////////////////////////////////////////////////////////////////////

ISensorProducerPtr CreateDiskSpaceSensorProducer(TCellMasterBootstrapConfigPtr config)
{
    return New<TDiskSpaceSensorProducer>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
