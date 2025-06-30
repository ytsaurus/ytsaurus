#include "huge_page_manager.h"

#include "config.h"
#include "public.h"
#include "private.h"

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/threading/spin_lock.h>

#ifdef _linux_
#include <sys/mman.h>
#endif

#include <fstream>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = IOLogger;

////////////////////////////////////////////////////////////////////////////////

static TErrorOr<void*> MapHugePageBlob(i64 size)
{
#ifdef _linux_
    void* page = mmap(
        nullptr,
        size,
        PROT_WRITE | PROT_READ,
        MAP_ANON | MAP_PRIVATE | MAP_HUGETLB,
        0,
        0);

    if (page == MAP_FAILED || !page) {
        return TError("Failed to allocate huge page")
            << TSystemError();
    }
#else
    YT_UNIMPLEMENTED();
#endif

    return page;
}

static void UnmapHugePageBlob(TMutableRef hugePageBlob)
{
#ifdef _linux_
    auto result = munmap(hugePageBlob.Begin(), hugePageBlob.Size());

    if (result == -1) {
        YT_LOG_ERROR(
            TSystemError(),
            "Failed to munmap huge page blob (Address: %p, Size: %v)",
            hugePageBlob.Begin(),
            hugePageBlob.Size());
    }
#else
    YT_UNIMPLEMENTED();
#endif
}

////////////////////////////////////////////////////////////////////////////////

class THugePageManager
    : public IHugePageManager
{
public:
    THugePageManager(
        THugePageManagerConfigPtr config,
        NProfiling::TProfiler profiler)
        : StaticConfig_(std::move(config))
        , DynamicConfig_(New<THugePageManagerDynamicConfig>())
        , Profiler_(std::move(profiler))
    {
        YT_VERIFY(StaticConfig_);

#ifdef _linux_
        std::ifstream memInfo("/proc/meminfo");
        std::string line;

        if (!memInfo.is_open()) {
            THROW_ERROR_EXCEPTION("Failed to open /proc/meminfo");
        }

        while (std::getline(memInfo, line)) {
            if (line.find("Hugepagesize:") != std::string::npos) {
                size_t kbSize = std::stoul(line.substr(line.find(":") + 1));
                HugePageSize_ = kbSize * 1024;
            } else if (line.find("HugePages_Total:") != std::string::npos) {
                HugePageCount_ = std::stoul(line.substr(line.find(":") + 1));
            }
        }
#endif

        Profiler_.AddFuncGauge("/huge_page_count", MakeStrong(this), [this] {
            return GetHugePageCount();
        });
        Profiler_.AddFuncGauge("/huge_page_size", MakeStrong(this), [this] {
            return GetHugePageSize();
        });
        Profiler_.AddFuncGauge("/used_huge_page_count", MakeStrong(this), [this] {
            return GetUsedHugePageCount();
        });
        Profiler_.AddFuncGauge("/huge_page_blob_size", MakeStrong(this), [this] {
            auto guard = Guard(Lock_);
            return GetHugeBlobSize();
        });
    }

    TErrorOr<TSharedMutableRef> ReserveHugePageBlob()
    {
        if (HugePageCount_ == 0) {
            return TError("Huge pages are not supported on this system, huge page count = 0");
        }

        if (HugePageSize_ == 0) {
            return TError("Huge pages are not supported on this system, huge page size = 0");
        }

        auto guard = Guard(Lock_);
        auto blobSize = GetHugeBlobSize();
        auto& freeBlobs = HugePageSizeToFreeBlobs_[blobSize];
        TSharedMutableRef hugeBlob;

        if (freeBlobs.empty()) {
            auto result = MapHugePageBlob(blobSize);

            if (!result.IsOK()) {
                return result.Wrap();
            }

            auto blob = result.Value();
            auto ref = TMutableRef(blob, blobSize);
            hugeBlob = TSharedMutableRef(ref, New<THugePageBlobHolder>(ref, MakeWeak(this)));
            UsedHugePageCount_ += GetHugePagePerBlob();
        } else {
            auto blobIt = freeBlobs.begin();
            auto blob = *blobIt;
            freeBlobs.erase(blobIt);
            hugeBlob = TSharedMutableRef(blob.second, New<THugePageBlobHolder>(blob.second, MakeWeak(this)));
        }

        return hugeBlob;
    }

    int GetHugePagePerBlob() const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);
        return DynamicConfig_->PagesPerBlob.value_or(StaticConfig_->PagesPerBlob);
    }

    size_t GetHugeBlobSize() const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);
        return GetHugePagePerBlob() * HugePageSize_;
    }

    int GetHugePageCount() const
    {
        return HugePageCount_;
    }

    int GetUsedHugePageCount() const
    {
        return UsedHugePageCount_;
    }

    int GetHugePageSize() const
    {
        return HugePageSize_;
    }

    bool IsEnabled() const
    {
        auto guard = Guard(Lock_);
        return DynamicConfig_->Enabled.value_or(StaticConfig_->Enabled);
    }

    void Reconfigure(const THugePageManagerDynamicConfigPtr& config)
    {
        YT_VERIFY(config);

        auto guard = Guard(Lock_);

        auto currentPagesPerBlob = GetHugePagePerBlob();
        if (config->PagesPerBlob != currentPagesPerBlob) {
            HugePageSizeToFreeBlobs_.erase(GetHugeBlobSize());
        }

        DynamicConfig_ = config;
    }

private:
    class THugePageBlobHolder
        : public TSharedRangeHolder
    {
    public:
        THugePageBlobHolder(
            TMutableRef data,
            TWeakPtr<THugePageManager> manager)
            : TSharedRangeHolder()
            , Data_(std::move(data))
            , Manager_(std::move(manager))
        { }

        ~THugePageBlobHolder() override
        {
            if (auto lockedManager = Manager_.Lock()) {
                lockedManager->UnlockHugePageBlob(Data_);
            } else {
                UnmapHugePageBlob(std::move(Data_));
            }
        }

    private:
        TMutableRef Data_;
        TWeakPtr<THugePageManager> Manager_;
    };

    const THugePageManagerConfigPtr StaticConfig_;
    THugePageManagerDynamicConfigPtr DynamicConfig_;
    const NProfiling::TProfiler Profiler_;

    i64 HugePageSize_ = 0;
    i64 HugePageCount_ = 0;
    i64 UsedHugePageCount_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    THashMap<i64, THashMap<char*, TMutableRef>> HugePageSizeToFreeBlobs_;

    void UnlockHugePageBlob(TMutableRef blob)
    {
        auto guard = Guard(Lock_);

        if (blob.Size() == GetHugeBlobSize()) {
            HugePageSizeToFreeBlobs_[blob.Size()].emplace(blob.Begin(), blob);
        } else {
            UsedHugePageCount_ -= blob.Size() / HugePageSize_;
            UnmapHugePageBlob(std::move(blob));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IHugePageManagerPtr CreateHugePageManager(
    THugePageManagerConfigPtr config,
    NProfiling::TProfiler profiler)
{
    return New<THugePageManager>(std::move(config), std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
