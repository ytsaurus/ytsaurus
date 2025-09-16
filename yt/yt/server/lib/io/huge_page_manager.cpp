#include "huge_page_manager.h"

#include "config.h"
#include "public.h"
#include "private.h"

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/threading/spin_lock.h>

#ifdef _linux_
#include <sys/mman.h>
#endif

namespace {

////////////////////////////////////////////////////////////////////////////////

std::optional<int> ReadParamFromMeminfo(std::string_view prefix)
{
#ifdef _linux_
    TFileInput memInfo("/proc/meminfo");
    TString line;

    while (memInfo.ReadLine(line)) {
        if (TStringBuf removedPrefix = line; removedPrefix.SkipPrefix(prefix)) {
            return std::stoul(std::string(removedPrefix));
        }
    }
#endif
    return std::nullopt;
}

int GetPreallocatedHugePageSize()
{
    return ReadParamFromMeminfo("HugePages_Total:").value_or(0);
}

int GetSystemHugePageSize()
{
    return ReadParamFromMeminfo("Hugepagesize:").value_or(0) * 1024;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(IHugePageAllocator)

class IHugePageAllocator
    : public TRefCounted
{
public:
    virtual TErrorOr<TMutableRef> AllocateHugePageBlob(int pages, const IHugePageManager& hugePageManager) = 0;
    virtual void DeallocateHugePageBlob(TMutableRef hugePageBlob) = 0;
};

DEFINE_REFCOUNTED_TYPE(IHugePageAllocator)

class THugePageManager
    : public IHugePageManager
{
public:
    THugePageManager(
        THugePageManagerConfigPtr config,
        NProfiling::TProfiler profiler,
        IHugePageAllocatorPtr hugePageAllocator)
        : StaticConfig_(std::move(config))
        , DynamicConfig_(New<THugePageManagerDynamicConfig>())
        , Profiler_(std::move(profiler))
        , HugePageAllocator_(std::move(hugePageAllocator))
        , HugePageSize_(GetSystemHugePageSize())
    {
        YT_VERIFY(StaticConfig_);

        Profiler_.AddFuncGauge("/huge_page_size", MakeStrong(this), [this] {
            return GetHugePageSize();
        });
        Profiler_.AddFuncGauge("/used_huge_page_count", MakeStrong(this), [this] {
            return GetUsedHugePageCount();
        });
        Profiler_.AddFuncGauge("/huge_page_blob_size", MakeStrong(this), [this] {
            return GetHugeBlobSize();
        });
        profiler.AddFuncGauge("/huge_page_memory", MakeStrong(this), [this] {
            return GetHugePageSize() * GetUsedHugePageCount();
        });
        profiler.AddFuncGauge("/huge_page_memory_limit", MakeStrong(this), [this] {
            return GetHugePageMemoryLimit();
        });
    }

    TErrorOr<TSharedMutableRef> ReserveHugePageBlob() override
    {
        auto guard = Guard(Lock_);
        auto blobSize = GetHugeBlobSize();
        auto& freeBlobs = HugePageSizeToFreeBlobs_[blobSize];
        TSharedMutableRef hugeBlob;

        if (freeBlobs.empty()) {
            YT_VERIFY(HugePageAllocator_);
            auto result = HugePageAllocator_->AllocateHugePageBlob(GetHugePagePerBlob(), *this);

            if (!result.IsOK()) {
                return result.Wrap();
            }

            auto blob = result.Value();
            hugeBlob = TSharedMutableRef(blob, New<THugePageBlobHolder>(blob, MakeWeak(this), HugePageAllocator_));
            UsedHugePageCount_ += GetHugePagePerBlob();
        } else {
            auto blob = freeBlobs.back();
            freeBlobs.pop_back();
            hugeBlob = TSharedMutableRef(blob, New<THugePageBlobHolder>(blob, MakeWeak(this), HugePageAllocator_));
        }

        return hugeBlob;
    }

    i64 GetHugePageMemoryLimit() const override
    {
        return DynamicConfig_.Acquire()->HugePageMemoryLimit.value_or(StaticConfig_->HugePageMemoryLimit);
    }

    int GetUsedHugePageCount() const override
    {
        return UsedHugePageCount_.load();
    }

    i64 GetHugePageSize() const override
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
    IHugePageAllocatorPtr HugePageAllocator_;

private:
    class THugePageBlobHolder
        : public TSharedRangeHolder
    {
    public:
        THugePageBlobHolder(
            TMutableRef data,
            TWeakPtr<THugePageManager> manager,
            IHugePageAllocatorPtr hugePageAllocator)
            : TSharedRangeHolder()
            , Data_(std::move(data))
            , Manager_(std::move(manager))
            , HugePageAllocator_(std::move(hugePageAllocator))
        {
            YT_VERIFY(HugePageAllocator_);
        }

        ~THugePageBlobHolder() override
        {
            if (auto lockedManager = Manager_.Lock()) {
                lockedManager->UnlockHugePageBlob(Data_);
            } else {
                HugePageAllocator_->DeallocateHugePageBlob(Data_);
            }
        }

    private:
        const TMutableRef Data_;
        const TWeakPtr<THugePageManager> Manager_;
        const IHugePageAllocatorPtr HugePageAllocator_;
    };

    i64 HugePageSize_ = 0;
    std::atomic<int> UsedHugePageCount_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    THashMap<i64, std::vector<TMutableRef>> HugePageSizeToFreeBlobs_;

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
            HugePageSizeToFreeBlobs_[blob.Size()].push_back(blob);
        } else {
            UsedHugePageCount_ -= blob.Size() / HugePageSize_;
            HugePageAllocator_->DeallocateHugePageBlob(std::move(blob));
        }
    }
};

class TPreallocatedHugePageAllocator
    : public IHugePageAllocator
{
public:
    TPreallocatedHugePageAllocator(i64 hugePageCount, const NProfiling::TProfiler& profiler)
        : HugePageCount_(hugePageCount)
    {
        profiler.AddFuncGauge("/huge_page_count", MakeStrong(this), [this] {
            return GetHugePageCount();
        });
    }

    TErrorOr<TMutableRef> AllocateHugePageBlob(int pages, const IHugePageManager& hugePageManager) override {
        auto usedHugePageCount = hugePageManager.GetUsedHugePageCount();
        auto hugePageSize = hugePageManager.GetHugePageSize();
        if (usedHugePageCount + pages > HugePageCount_) {
            return TError("Not enough huge pages")
                << TErrorAttribute("requested_huge_pages", pages)
                << TErrorAttribute("available_huge_pages", HugePageCount_)
                << TErrorAttribute("used_huge_pages", usedHugePageCount);
        }
#ifdef _linux_
        void* page = mmap(
            nullptr,
            pages * hugePageSize,
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

        return TMutableRef(page, pages * hugePageSize);
    }

    void DeallocateHugePageBlob(TMutableRef hugePageBlob) override {
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

    i64 GetHugePageCount() const {
        return HugePageCount_;
    }

private:
    i64 HugePageCount_ = 0;
};

class TTransparentHugePageAllocator
    : public IHugePageAllocator
{
public:
    TErrorOr<TMutableRef> AllocateHugePageBlob(int pages, const IHugePageManager& hugePageManager) override {
        auto hugePageSize = hugePageManager.GetHugePageSize();
        auto freeHugePageMemory = GetFreeHugePageMemory(hugePageManager);
        if (freeHugePageMemory < pages * hugePageSize) {
            return TError("Not enough huge pages")
                << TErrorAttribute("requested_huge_pages", pages)
                << TErrorAttribute("available_huge_page_memory", freeHugePageMemory)
                << TErrorAttribute("used_huge_page_memory", GetHugePageMemory(hugePageManager));
        }
#ifdef _linux_
        void* page = ::aligned_malloc(pages * hugePageSize, hugePageSize);

        if (!page) {
            return TError("Failed to allocate aligned huge page")
                << TSystemError();
        }

        if (::madvise(page, pages * hugePageSize, MADV_HUGEPAGE) != 0) {
            return TError("Failed to mark memory MADV_HUGEPAGE")
                << TSystemError(errno);
        }
#else
        YT_UNIMPLEMENTED();
#endif

        return TMutableRef(page, pages * hugePageSize);
    }

    void DeallocateHugePageBlob(TMutableRef hugePageBlob) override {
#ifdef _linux_
        ::madvise(hugePageBlob.Begin(), hugePageBlob.Size(), MADV_DONTNEED);
        ::free(hugePageBlob.Begin());
#else
        YT_UNIMPLEMENTED();
#endif
    }

private:
    int GetFreeHugePageMemory(const IHugePageManager& hugePageManager) const
    {
        return hugePageManager.GetHugePageMemoryLimit() - GetHugePageMemory(hugePageManager);
    }

    int GetHugePageMemory(const IHugePageManager& hugePageManager) const
    {
        return hugePageManager.GetUsedHugePageCount() * hugePageManager.GetHugePageSize();
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
        , PreallocatedHugePageAllocator_(New<TPreallocatedHugePageAllocator>(GetPreallocatedHugePageSize(), Profiler_))
        , TransparentHugePageAllocator_(New<TTransparentHugePageAllocator>())
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

    i64 GetHugePageMemoryLimit() const override
    {
        return HugePageManager_.Acquire()->GetHugePageSize();
    }

    i64 GetHugePageSize() const override
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

    const TIntrusivePtr<TPreallocatedHugePageAllocator> PreallocatedHugePageAllocator_;
    const TIntrusivePtr<TTransparentHugePageAllocator> TransparentHugePageAllocator_;

    TAtomicIntrusivePtr<IHugePageManager> HugePageManager_;

    EHugeManagerType GetCurrentType() const
    {
        return DynamicConfig_.Acquire()->Type.value_or(StaticConfig_->Type);
    }

    IHugePageManagerPtr CreateHugePageManager(EHugeManagerType type)
    {
        return New<THugePageManager>(StaticConfig_, Profiler_, CreateHugePageAllocator(type));
    }

    IHugePageAllocatorPtr CreateHugePageAllocator(EHugeManagerType type)
    {
        switch (type) {
            case EHugeManagerType::Preallocated:
                YT_VERIFY(PreallocatedHugePageAllocator_);
                return PreallocatedHugePageAllocator_;
            case EHugeManagerType::Transparent:
                YT_VERIFY(TransparentHugePageAllocator_);
                return TransparentHugePageAllocator_;
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
