#ifndef HELPERS_INL_H
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

template <class TProtoDiskRequest>
void FromProto(TNbdDiskRequest* diskRequestConfig, const TProtoDiskRequest& protoDiskRequestConfig)
{
    diskRequestConfig->NbdDisk = New<TNbdDiskConfig>();

    if constexpr (std::is_same_v<TProtoDiskRequest, NProto::TNbdDiskRequest>) {
        FromProto(static_cast<TDiskRequestConfig*>(diskRequestConfig), protoDiskRequestConfig.disk_request());
        FromProto(&(*diskRequestConfig->NbdDisk), protoDiskRequestConfig.nbd());
    } else {
        static_assert(std::is_same_v<TProtoDiskRequest, NProto::TDeprecatedDiskRequest>);
        FromProto(static_cast<TDiskRequestConfig*>(diskRequestConfig), protoDiskRequestConfig);
        FromProto(&(*diskRequestConfig->NbdDisk), protoDiskRequestConfig.nbd_disk());
    }
}

template <class TProtoDiskRequest>
void ToProto(TProtoDiskRequest* protoDiskRequestConfig, const TNbdDiskRequest& diskRequestConfig)
{
    if constexpr (std::is_same_v<TProtoDiskRequest, NProto::TNbdDiskRequest>) {
        ToProto(protoDiskRequestConfig->mutable_disk_request(), static_cast<const TDiskRequestConfig&>(diskRequestConfig));
        ToProto(protoDiskRequestConfig->mutable_nbd(), *diskRequestConfig.NbdDisk);
    } else {
        static_assert(std::is_same_v<TProtoDiskRequest, NProto::TDeprecatedDiskRequest>);
        ToProto(protoDiskRequestConfig, static_cast<const TDiskRequestConfig&>(diskRequestConfig));
        ToProto(protoDiskRequestConfig->mutable_nbd_disk(), *diskRequestConfig.NbdDisk);
    }
}

template <class TProtoDiskRequest>
void FromProto(TLocalDiskRequest* diskRequestConfig, const TProtoDiskRequest& protoDiskRequestConfig)
{
    if constexpr (std::is_same_v<TProtoDiskRequest, NProto::TLocalDiskRequest>) {
        FromProto(static_cast<TDiskRequestConfig*>(diskRequestConfig), protoDiskRequestConfig.disk_request());
    } else {
        static_assert(std::is_same_v<TProtoDiskRequest, NProto::TDeprecatedDiskRequest>);
        FromProto(static_cast<TDiskRequestConfig*>(diskRequestConfig), protoDiskRequestConfig);
    }
}

template <class TProtoDiskRequest>
void ToProto(TProtoDiskRequest* protoDiskRequestConfig, const TLocalDiskRequest& diskRequestConfig)
{
    if constexpr (std::is_same_v<TProtoDiskRequest, NProto::TLocalDiskRequest>) {
        ToProto(protoDiskRequestConfig->mutable_disk_request(), static_cast<const TDiskRequestConfig&>(diskRequestConfig));
    } else {
        static_assert(std::is_same_v<TProtoDiskRequest, NProto::TDeprecatedDiskRequest>);
        ToProto(protoDiskRequestConfig, static_cast<const TDiskRequestConfig&>(diskRequestConfig));
    }
}

template <class TProtoDiskRequest>
void FromProto(TDiskRequestConfig* diskRequestConfig, const TProtoDiskRequest& protoDiskRequestConfig)
{
    if constexpr (std::is_same_v<TProtoDiskRequest, NProto::TDiskRequest>) {
        FromProto(static_cast<TStorageRequestBase*>(diskRequestConfig), protoDiskRequestConfig.storage_request_common_parameters());
    } else {
        static_assert(std::is_same_v<TProtoDiskRequest, NProto::TDeprecatedDiskRequest>);
        FromProto(static_cast<TStorageRequestBase*>(diskRequestConfig), protoDiskRequestConfig);
    }

    if (protoDiskRequestConfig.has_inode_count()) {
        diskRequestConfig->InodeCount = protoDiskRequestConfig.inode_count();
    }

    if (protoDiskRequestConfig.has_medium_index()) {
        diskRequestConfig->MediumIndex = protoDiskRequestConfig.medium_index();
    }
}

template <class TProtoDiskRequest>
void ToProto(TProtoDiskRequest* protoDiskRequestConfig, const TDiskRequestConfig& diskRequestConfig)
{
    if constexpr (std::is_same_v<TProtoDiskRequest, NProto::TDiskRequest>) {
        ToProto(protoDiskRequestConfig->mutable_storage_request_common_parameters(), static_cast<const TStorageRequestBase&>(diskRequestConfig));
    } else {
        static_assert(std::is_same_v<TProtoDiskRequest, NProto::TDeprecatedDiskRequest>);
        ToProto(protoDiskRequestConfig, static_cast<const TStorageRequestBase&>(diskRequestConfig));
    }

    if (diskRequestConfig.InodeCount) {
        protoDiskRequestConfig->set_inode_count(*diskRequestConfig.InodeCount);
    }

    if (diskRequestConfig.MediumIndex) {
        protoDiskRequestConfig->set_medium_index(*diskRequestConfig.MediumIndex);
    }
}

template <class TProtoDiskRequest>
void FromProto(TStorageRequestBase* diskRequestConfig, const TProtoDiskRequest& protoDiskRequestConfig)
{
    diskRequestConfig->DiskSpace = protoDiskRequestConfig.disk_space();
}

template <class TProtoDiskRequest>
void ToProto(TProtoDiskRequest* protoDiskRequestConfig, const TStorageRequestBase& diskRequestConfig)
{
    protoDiskRequestConfig->set_disk_space(diskRequestConfig.DiskSpace);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
