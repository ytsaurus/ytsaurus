#ifndef HELPERS_INL_H
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

template <class TProtoDiskRequest>
void BuildNbdDiskRequestSpec(TProtoDiskRequest* protoDiskRequestConfig, const TNbdDiskRequest& diskRequestConfig)
{
    if constexpr (std::is_same_v<TProtoDiskRequest, NProto::TNbdDiskRequest>) {
        BuildCommonDiskRequestSpec(protoDiskRequestConfig->mutable_disk_request(), static_cast<const TDiskRequestConfig&>(diskRequestConfig));
        BuildChunkNbdDiskSpec(protoDiskRequestConfig->mutable_chunk_nbd(), *diskRequestConfig.NbdDisk);
    } else {
        static_assert(std::is_same_v<TProtoDiskRequest, NProto::TDeprecatedDiskRequest>);
        BuildCommonDiskRequestSpec(protoDiskRequestConfig, static_cast<const TDiskRequestConfig&>(diskRequestConfig));
        BuildChunkNbdDiskSpec(protoDiskRequestConfig->mutable_chunk_nbd_disk(), *diskRequestConfig.NbdDisk);
    }
}

template <class TProtoDiskRequest>
void BuildLocalDiskRequestSpec(TProtoDiskRequest* protoDiskRequestConfig, const TLocalDiskRequest& diskRequestConfig)
{
    if constexpr (std::is_same_v<TProtoDiskRequest, NProto::TLocalDiskRequest>) {
        BuildCommonDiskRequestSpec(protoDiskRequestConfig->mutable_disk_request(), static_cast<const TDiskRequestConfig&>(diskRequestConfig));
    } else {
        static_assert(std::is_same_v<TProtoDiskRequest, NProto::TDeprecatedDiskRequest>);
        BuildCommonDiskRequestSpec(protoDiskRequestConfig, static_cast<const TDiskRequestConfig&>(diskRequestConfig));
    }
}

template <class TProtoDiskRequest>
void BuildCommonDiskRequestSpec(TProtoDiskRequest* protoDiskRequestConfig, const TDiskRequestConfig& diskRequestConfig)
{
    if constexpr (std::is_same_v<TProtoDiskRequest, NProto::TDiskRequest>) {
        BuildCommonStorageRequestSpec(protoDiskRequestConfig->mutable_storage_request_common_parameters(), static_cast<const TStorageRequestBase&>(diskRequestConfig));
    } else {
        static_assert(std::is_same_v<TProtoDiskRequest, NProto::TDeprecatedDiskRequest>);
        BuildCommonStorageRequestSpec(protoDiskRequestConfig, static_cast<const TStorageRequestBase&>(diskRequestConfig));
    }

    if (diskRequestConfig.InodeCount) {
        protoDiskRequestConfig->set_inode_count(*diskRequestConfig.InodeCount);
    }

    if (diskRequestConfig.MediumIndex) {
        protoDiskRequestConfig->set_medium_index(*diskRequestConfig.MediumIndex);
    }
}

template <class TProtoDiskRequest>
void BuildCommonStorageRequestSpec(TProtoDiskRequest* protoDiskRequestConfig, const TStorageRequestBase& diskRequestConfig)
{
    protoDiskRequestConfig->set_disk_space(diskRequestConfig.DiskSpace);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
