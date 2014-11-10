#include "stdafx.h"
#include "blob.h"
#include "ref.h"
#include "ref_counted_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const size_t InitialBlobCapacity = 16;
const double BlobCapacityMultiplier = 2.0;

TBlob::TBlob(TRefCountedTypeCookie tagCookie, size_t size, bool initiailizeStorage)
{
    SetTypeCookie(tagCookie);
    if (size == 0) {
        Reset();
    } else {
        Allocate(std::max(size, InitialBlobCapacity));
        Size_ = size;
        if (initiailizeStorage) {
            memset(Begin_, 0, Size_);
        }
    }
}

TBlob::TBlob(TRefCountedTypeCookie tagCookie, const void* data, size_t size)
{
    SetTypeCookie(tagCookie);
    Reset();
    Append(data, size);
}

TBlob::TBlob(const TBlob& other)
{
    SetTypeCookie(other);
    if (other.Size_ == 0) {
        Reset();
    } else {
        Allocate(std::max(InitialBlobCapacity, other.Size_));
        memcpy(Begin_, other.Begin_, other.Size_);
        Size_ = other.Size_;
    }
}

TBlob::TBlob(TBlob&& other)
    : Begin_(other.Begin_)
    , Size_(other.Size_)
    , Capacity_(other.Capacity_)
{
    SetTypeCookie(other);
    other.Reset();
}

TBlob::~TBlob()
{
    Free();
}

void TBlob::Reserve(size_t newCapacity)
{
    if (newCapacity > Capacity_) {
        Reallocate(newCapacity);
    }
}

void TBlob::Resize(size_t newSize, bool initializeStorage /*= true*/)
{
    if (newSize > Size_) {
        if (newSize > Capacity_) {
            size_t newCapacity;
            if (Capacity_ == 0) {
                newCapacity = std::max(InitialBlobCapacity, newSize);
            } else {
                newCapacity = std::max(static_cast<size_t>(Capacity_ * BlobCapacityMultiplier), newSize);
            }
            Reallocate(newCapacity);
        }
        if (initializeStorage) {
            memset(Begin_ + Size_, 0, newSize - Size_);
        }
    }
    Size_ = newSize;
}

TBlob& TBlob::operator = (const TBlob& rhs)
{
    if (this != &rhs) {
        Free();
        SetTypeCookie(rhs);
        if (rhs.Size_ == 0) {
            Reset();
        } else {
            Allocate(std::max(InitialBlobCapacity, rhs.Size_));
            memcpy(Begin_, rhs.Begin_, rhs.Size_);
            Size_ = rhs.Size_;
        }
    }
    return *this;
}

TBlob& TBlob::operator = (TBlob&& rhs)
{
    if (this != &rhs) {
        Free();
        SetTypeCookie(rhs);
        Begin_ = rhs.Begin_;
        Size_ = rhs.Size_;
        Capacity_ = rhs.Capacity_;
        rhs.Reset();
    }
    return *this;
}

void TBlob::Append(const void* data, size_t size)
{
    if (Size_ + size > Capacity_) {
        Resize(Size_ + size, false);
        memcpy(Begin_ + Size_ - size, data, size);
    } else {
        memcpy(Begin_ + Size_, data, size);
        Size_ += size;
    }
}

void TBlob::Append(const TRef& ref)
{
    Append(ref.Begin(), ref.Size());
}

void TBlob::Append(char ch)
{
    if (Size_ + 1 > Capacity_) {
        Resize(Size_ + 1, false);
        Begin_[Size_ - 1] = ch;
    } else {
        Begin_[Size_++] = ch;
    }
}

void TBlob::Reset()
{
    Begin_ = nullptr;
    Size_ = Capacity_ = 0;
}

void TBlob::Allocate(size_t newCapacity)
{
    Begin_ = new char[newCapacity];
    Capacity_ = newCapacity;
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TRefCountedTracker::Get()->Allocate(TypeCookie_, newCapacity);
#endif
}
void TBlob::Reallocate(size_t newCapacity)
{
    if (Begin_ == nullptr) {
        Allocate(newCapacity);
        return;
    }
    char* newBegin = new char[newCapacity];
    memcpy(newBegin, Begin_, Size_);
    delete[] Begin_;
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TRefCountedTracker::Get()->Reallocate(TypeCookie_, Capacity_, newCapacity);
#endif
    Begin_ = newBegin;
    Capacity_ = newCapacity;
}

void TBlob::Free()
{
    if (Begin_ == nullptr) {
        return;
    }
    delete[] Begin_;
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TRefCountedTracker::Get()->Free(TypeCookie_, Capacity_);
#endif
}

void TBlob::SetTypeCookie(TRefCountedTypeCookie tagCookie)
{
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TypeCookie_ = tagCookie;
#endif
}
void TBlob::SetTypeCookie(const TBlob& other)
{
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TypeCookie_ = other.TypeCookie_;
#endif
}

void swap(TBlob& left, TBlob& right)
{
    if (&left != &right) {
        std::swap(left.Begin_, right.Begin_);
        std::swap(left.Size_, right.Size_);
        std::swap(left.Capacity_, right.Capacity_);
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        std::swap(left.TypeCookie_, right.TypeCookie_);
#endif
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
