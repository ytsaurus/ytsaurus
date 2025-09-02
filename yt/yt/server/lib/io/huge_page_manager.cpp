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

class THugePageManagerBase
    : public IHugePageManager
{
public:
    THugePageManagerBase(
        THugePageManagerConfigPtr config,
        NProfiling::TProfiler profiler)
        : StaticConfig_(std::move(config))
        , DynamicConfig_(New<THugePageManagerDynamicConfig>())
        , Profiler_(std::move(profiler))
    {
        YT_VERIFY(StaticConfig_);

#ifdef _linux_
        TFileInput memInfo("/proc/meminfo");
        TString line;

        while (memInfo.ReadLine(line)) {
            if (line.find("Hugepagesize:") != std::string::npos) {
                size_t kbSize = std::stoul(line.substr(line.find(":") + 1));
                HugePageSize_ = kbSize * 1024;
                break;
            }
        }
#endif

        Profiler_.AddFuncGauge("/huge_page_size", MakeStrong(this), [this] {
            return GetHugePageSize();
        });
        Profiler_.AddFuncGauge("/used_huge_page_count", MakeStrong(this), [this] {
            return GetUsedHugePageCount();
        });
        Profiler_.AddFuncGauge("/huge_page_blob_size", MakeStrong(this), [this] {
            return GetHugeBlobSize();
        });
    }

    TErrorOr<TSharedMutableRef> ReserveHugePageBlob() override
    {
        auto guard = Guard(Lock_);
        auto blobSize = GetHugeBlobSize();
        auto& freeBlobs = HugePageSizeToFreeBlobs_[blobSize];
        TSharedMutableRef hugeBlob;

        if (freeBlobs.empty()) {
            auto result = AllocateHugePageBlob(GetHugePagePerBlob());

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

    int GetUsedHugePageCount() const override
    {
        return UsedHugePageCount_.load();
    }

    int GetHugePageSize() const override
    {
        return HugePageSize_;
    }

    bool IsEnabled() const override
    {
        return DynamicConfig_.Acquire()->Enabled.value_or(StaticConfig_->Enabled);
    }

    void Reconfigure(const THugePageManagerDynamicConfigPtr& config) override
    {
        YT_VERIFY(config);

        auto guard = Guard(Lock_);

        auto currentPagesPerBlob = GetHugePagePerBlob();
        if (config->PagesPerBlob != currentPagesPerBlob) {
            HugePageSizeToFreeBlobs_.erase(GetHugeBlobSize());
        }

        DynamicConfig_ = config;
    }

protected:
    const THugePageManagerConfigPtr StaticConfig_;
    TAtomicIntrusivePtr<THugePageManagerDynamicConfig> DynamicConfig_;
    const NProfiling::TProfiler Profiler_;

private:
    class THugePageBlobHolder
        : public TSharedRangeHolder
    {
    public:
        THugePageBlobHolder(
            TMutableRef data,
            TWeakPtr<THugePageManagerBase> manager)
            : TSharedRangeHolder()
            , Data_(std::move(data))
            , Manager_(std::move(manager))
        { }

        ~THugePageBlobHolder() override
        {
            if (auto lockedManager = Manager_.Lock()) {
                lockedManager->UnlockHugePageBlob(Data_);
            } else {
                lockedManager->DeallocateHugePageBlob(Data_);
            }
        }

    private:
        const TMutableRef Data_;
        const TWeakPtr<THugePageManagerBase> Manager_;
    };

    i64 HugePageSize_ = 0;
    std::atomic<int> UsedHugePageCount_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    THashMap<i64, THashMap<char*, TMutableRef>> HugePageSizeToFreeBlobs_;

    virtual TErrorOr<void*> AllocateHugePageBlob(int pages) = 0;
    virtual void DeallocateHugePageBlob(const TMutableRef& hugePageBlob) = 0;

    int GetHugePagePerBlob() const
    {
        return DynamicConfig_.Acquire()->PagesPerBlob.value_or(StaticConfig_->PagesPerBlob);
    }

    i64 GetHugeBlobSize() const
    {
        return GetHugePagePerBlob() * HugePageSize_;
    }

    void UnlockHugePageBlob(TMutableRef blob)
    {
        auto guard = Guard(Lock_);

        if (std::ssize(blob) == GetHugeBlobSize()) {
            HugePageSizeToFreeBlobs_[blob.Size()].emplace(blob.Begin(), blob);
        } else {
            UsedHugePageCount_ -= blob.Size() / HugePageSize_;
            DeallocateHugePageBlob(std::move(blob));
        }
    }
};

class TPreallocatedHugePageManager
    : public THugePageManagerBase
{
public:
    TPreallocatedHugePageManager(
        THugePageManagerConfigPtr config,
        NProfiling::TProfiler profiler)
        : THugePageManagerBase(std::move(config), std::move(profiler))
    {
#ifdef _linux_
        TFileInput memInfo("/proc/meminfo");
        TString line;

        while (memInfo.ReadLine(line)) {
            if (line.find("HugePages_Total:") != std::string::npos) {
                HugePageCount_ = std::stoul(line.substr(line.find(":") + 1));
            }
        }
#endif

        Profiler_.AddFuncGauge("/huge_page_count", MakeStrong(this), [this] {
            return GetHugePageCount();
        });
    }

private:
    i64 HugePageCount_ = 0;

    TErrorOr<void*> AllocateHugePageBlob(int pages) override {
        if (GetUsedHugePageCount() + pages > HugePageCount_) {
            return TError("Not enough huge pages")
                << TErrorAttribute("requested_huge_pages", pages)
                << TErrorAttribute("available_huge_pages", HugePageCount_)
                << TErrorAttribute("used_huge_pages", GetUsedHugePageCount());
        }
#ifdef _linux_
        void* page = mmap(
            nullptr,
            pages * GetHugePageSize(),
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

    void DeallocateHugePageBlob(const TMutableRef& hugePageBlob) override {
#ifdef _linux_
        auto result = munmap(hugePageBlob.Begin(), hugePageBlob.Size());

        const auto Logger = IOLogger;
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

    int GetHugePageCount() const
    {
        return HugePageCount_;
    }
};

class TTransparentHugePageManager
    : public THugePageManagerBase
{
public:
    TTransparentHugePageManager(
        THugePageManagerConfigPtr config,
        NProfiling::TProfiler profiler)
        : THugePageManagerBase(std::move(config), std::move(profiler))
    {
        Profiler_.AddFuncGauge("/huge_page_memory_limit", MakeStrong(this), [this] {
            return GetHugePageMemoryLimit();
        });
        Profiler_.AddFuncGauge("/huge_page_memory", MakeStrong(this), [this] {
            return GetHugePageMemory();
        });
    }

private:
    TErrorOr<void*> AllocateHugePageBlob(int pages) override {
        if (GetFreeHugePageMemory() < pages * GetHugePageSize()) {
            return TError("Not enough huge pages")
                << TErrorAttribute("requested_huge_pages", pages)
                << TErrorAttribute("available_huge_page_memory", GetFreeHugePageMemory())
                << TErrorAttribute("used_huge_page_memory", GetHugePageMemory());
        }
#ifdef _linux_
        void* page = ::aligned_malloc(pages * GetHugePageSize(), GetHugePageSize());

        if (!page) {
            return TError("Failed to allocate aligned huge page")
                << TSystemError();
        }

        if (::madvise(page, pages * GetHugePageSize(), MADV_HUGEPAGE) != 0) {
            return TError("Failed to mark memory MADV_HUGEPAGE")
                << TSystemError(errno);
        }
#else
        YT_UNIMPLEMENTED();
#endif

        return page;
    }

    void DeallocateHugePageBlob(const TMutableRef& hugePageBlob) override {
#ifdef _linux_
        ::free(hugePageBlob.Begin());
#else
        YT_UNIMPLEMENTED();
#endif
    }

    int GetFreeHugePageMemory() const
    {
        return GetHugePageMemoryLimit() - GetHugePageMemory();
    }

    int GetHugePageMemory() const
    {
        return GetUsedHugePageCount() * GetHugePageSize();
    }

    int GetHugePageMemoryLimit() const
    {
        return DynamicConfig_.Acquire()->HugePageMemoryLimit.value_or(StaticConfig_->HugePageMemoryLimit);
    }
};

class TDynamicHugePageManager
    : public IHugePageManager
{
public:
    TDynamicHugePageManager(
        THugePageManagerConfigPtr config,
        NProfiling::TProfiler profiler)
        : StaticConfig_(std::move(config))
        , DynamicConfig_(New<THugePageManagerDynamicConfig>())
        , Profiler_(std::move(profiler))
        , HugePageManager_(CreateHugePageManager(GetCurrentType()))
    {
        Profiler_.AddFuncGauge("/current_huge_page_manager_type", MakeStrong(this), [this] {
            return static_cast<int>(GetCurrentType());
        });
    }

    TErrorOr<TSharedMutableRef> ReserveHugePageBlob() override
    {
        return HugePageManager_.Acquire()->ReserveHugePageBlob();
    }

    bool IsEnabled() const override
    {
        return HugePageManager_.Acquire()->IsEnabled();
    }

    int GetUsedHugePageCount() const override
    {
        return HugePageManager_.Acquire()->GetUsedHugePageCount();
    }

    int GetHugePageSize() const override
    {
        return HugePageManager_.Acquire()->GetHugePageSize();
    }

    void Reconfigure(const THugePageManagerDynamicConfigPtr& config) override
    {
        YT_VERIFY(config);

        auto previousType = GetCurrentType();

        DynamicConfig_ = std::move(config);

        if (previousType != GetCurrentType()) {
            HugePageManager_ = CreateHugePageManager(GetCurrentType());
        }

        HugePageManager_.Acquire()->Reconfigure(DynamicConfig_.Acquire());
    }

private:
    const THugePageManagerConfigPtr StaticConfig_;
    TAtomicIntrusivePtr<THugePageManagerDynamicConfig> DynamicConfig_;
    const NProfiling::TProfiler Profiler_;

    TAtomicIntrusivePtr<IHugePageManager> HugePageManager_;

    EHugeManagerType GetCurrentType() const
    {
        return DynamicConfig_.Acquire()->Type.value_or(StaticConfig_->Type);
    }

    IHugePageManagerPtr CreateHugePageManager(EHugeManagerType type)
    {
        switch (type) {
            case EHugeManagerType::Preallocated:
                return New<TPreallocatedHugePageManager>(StaticConfig_, Profiler_);
            case EHugeManagerType::Transparent:
                return New<TTransparentHugePageManager>(StaticConfig_, Profiler_);
        }

        YT_UNREACHABLE();
    }
};

////////////////////////////////////////////////////////////////////////////////

IHugePageManagerPtr CreateHugePageManager(
    THugePageManagerConfigPtr config,
    NProfiling::TProfiler profiler)
{
    return New<TDynamicHugePageManager>(std::move(config), std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
