#include "memory_helpers.h"

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////
TMemoryHolderBase::TMemoryHolderBase(TMemoryHolderBase&& other)
    : Ptr_(other.Ptr_)
{
    other.Ptr_ = nullptr;
}

void TMemoryHolderBase::operator=(TMemoryHolderBase&& other)
{
    Reset();
    Ptr_ = other.Ptr_;
    other.Ptr_ = nullptr;
}

void TMemoryHolderBase::Reset()
{
    if (Ptr_) {
        auto* header = GetHeader();
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        auto cookie = header->Cookie;
        if (cookie != NullRefCountedTypeCookie) {
            TRefCountedTrackerFacade::FreeTagInstance(cookie);
            TRefCountedTrackerFacade::FreeSpace(cookie, header->Size);
        }
#endif
        ::free(header);
        Ptr_ = nullptr;
    }
}

// NB: Resize does not preserve current values.
void* TMemoryHolderBase::Resize(size_t total, TRefCountedTypeCookie cookie)
{
    if (total > GetSize()) {
        Reset();
        total *= 2;
        auto ptr = ::malloc(sizeof(THeader) + total);

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        if (cookie != NullRefCountedTypeCookie) {
            TRefCountedTrackerFacade::AllocateTagInstance(cookie);
            TRefCountedTrackerFacade::AllocateSpace(cookie, total);
        }
#endif
        YT_VERIFY(reinterpret_cast<uintptr_t>(ptr) % 8 == 0);
        auto header = static_cast<THeader*>(ptr);
        *header = {total, cookie};
        Ptr_ = header + 1;
    }

    return Ptr_;
}

TMemoryHolderBase::~TMemoryHolderBase()
{
    Reset();
}

TMemoryHolderBase::THeader* TMemoryHolderBase::GetHeader() const
{
    return reinterpret_cast<THeader*>(Ptr_) - 1;
}

size_t TMemoryHolderBase::GetSize() const
{
    return Ptr_ ? GetHeader()->Size : 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
