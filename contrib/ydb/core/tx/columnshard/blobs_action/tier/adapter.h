#pragma once

#include <contrib/ydb/core/tx/columnshard/counters/error_collector.h>
#include <contrib/ydb/core/tx/columnshard/counters/indexation.h>
#include <contrib/ydb/core/wrappers/abstract.h>

#include <memory.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TRepliesAdapter: public NWrappers::NExternalStorage::IReplyAdapter {
private:
    const std::shared_ptr<NKikimr::NColumnShard::TErrorCollector> ErrorCollector;
    const TString StorageId;

public:
    TRepliesAdapter(const std::shared_ptr<NKikimr::NColumnShard::TErrorCollector>& errorCollector, const TString& storageId)
        : ErrorCollector(std::move(errorCollector))
        , StorageId(std::move(storageId)) {
    }

    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(
        std::unique_ptr<NWrappers::NExternalStorage::TEvGetObjectResponse>&& ev) const override;
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(
        std::unique_ptr<NWrappers::NExternalStorage::TEvPutObjectResponse>&& ev) const override;
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(
        std::unique_ptr<NWrappers::NExternalStorage::TEvListObjectsResponse>&& ev) const override {
        Y_UNUSED(ev);
        Y_ABORT_UNLESS(false);
    }
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(
        std::unique_ptr<NWrappers::NExternalStorage::TEvHeadObjectResponse>&& ev) const override {
        Y_UNUSED(ev);
        Y_ABORT_UNLESS(false);
    }
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(
        std::unique_ptr<NWrappers::NExternalStorage::TEvDeleteObjectResponse>&& ev) const override {
        return std::move(ev);
    }
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(
        std::unique_ptr<NWrappers::NExternalStorage::TEvDeleteObjectsResponse>&& ev) const override {
        Y_UNUSED(ev);
        Y_ABORT_UNLESS(false);
    }
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(
        std::unique_ptr<NWrappers::NExternalStorage::TEvCreateMultipartUploadResponse>&& ev) const override {
        Y_UNUSED(ev);
        Y_ABORT_UNLESS(false);
    }
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(
        std::unique_ptr<NWrappers::NExternalStorage::TEvUploadPartResponse>&& ev) const override {
        Y_UNUSED(ev);
        Y_ABORT_UNLESS(false);
    }
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(
        std::unique_ptr<NWrappers::NExternalStorage::TEvCompleteMultipartUploadResponse>&& ev) const override {
        Y_UNUSED(ev);
        Y_ABORT_UNLESS(false);
    }
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(
        std::unique_ptr<NWrappers::NExternalStorage::TEvAbortMultipartUploadResponse>&& ev) const override {
        Y_UNUSED(ev);
        Y_ABORT_UNLESS(false);
    }
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(
        std::unique_ptr<NWrappers::NExternalStorage::TEvCheckObjectExistsResponse>&& ev) const override {
        Y_UNUSED(ev);
        Y_ABORT_UNLESS(false);
    }
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(
        std::unique_ptr<NWrappers::NExternalStorage::TEvUploadPartCopyResponse>&& ev) const override {
        Y_UNUSED(ev);
        Y_ABORT_UNLESS(false);
    }
};

}   // namespace NKikimr::NOlap::NBlobOperations::NTier
