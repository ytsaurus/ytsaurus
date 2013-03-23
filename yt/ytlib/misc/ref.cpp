#include "stdafx.h"
#include "ref.h"
#include "ref_counted_tracker.h"

#include <util/stream/str.h>

#include <util/system/info.h>

#include <util/ysaveload.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static const size_t InitialBlobCapacity = 16;
static const double BlobCapacityMultiplier = 2.0;

TBlob::TBlob()
{
    Reset();
}

TBlob::TBlob(size_t size, bool initiailizeStorage /*= true*/)
{
    if (size == 0) {
        Reset();
    } else {
        Begin_ = new char[size];
        Size_ = Capacity_ = size;
        if (initiailizeStorage) {
            memset(Begin_, 0, Size_);
        }
    }
}

TBlob::TBlob(const TBlob& other)
{
    if (other.Size_ == 0) {
        Reset();
    } else {
        Begin_ = new char[other.Size_];
        memcpy(Begin_, other.Begin_, other.Size_);
        Size_ = Capacity_ = other.Size_;
    }
}

TBlob::TBlob(TBlob&& other)
    : Begin_(other.Begin_)
    , Size_(other.Size_)
    , Capacity_(other.Capacity_)
{
    other.Reset();
}

TBlob::TBlob(const void* data, size_t size)
{
    Reset();
    Append(data, size);
}

TBlob::~TBlob()
{
    delete[] Begin_;
}

void TBlob::Reserve(size_t newCapacity)
{
    if (newCapacity > Capacity_) {
        char* newBegin = new char[newCapacity];
        memcpy(newBegin, Begin_, Size_);
        delete[] Begin_;
        Begin_ = newBegin;
        Capacity_ = newCapacity;
    }
}

void TBlob::Resize(size_t newSize, bool initializeStorage /*= true*/)
{
    if (newSize > Size_) {
        if (newSize > Capacity_) {
            if (Capacity_ == 0) {
                Capacity_ = std::max(InitialBlobCapacity, newSize);
            } else {
                Capacity_ = std::max(static_cast<size_t>(Capacity_ * BlobCapacityMultiplier), newSize);
            }
            char* newBegin = new char[Capacity_];
            memcpy(newBegin, Begin_, Size_);
            delete[] Begin_;
            Begin_ = newBegin;
        }
        if (initializeStorage) {
            memset(Begin_ + Size_, 0, newSize - Size_);
        }
    }
    Size_ = newSize;
}

void TBlob::Clear()
{
    delete[] Begin_;
    Reset();
}

TBlob& TBlob::operator = (const TBlob& rhs)
{
    if (this != &rhs) {
        delete[] Begin_;
        Begin_ = new char[rhs.Size_];
        memcpy(Begin_, rhs.Begin_, rhs.Size_);
        Size_ = Capacity_ = rhs.Size_;
    }
    return *this;
}

TBlob& TBlob::operator = (TBlob&& rhs)
{
    if (this != &rhs) {
        Begin_ = rhs.Begin_;
        Size_ = rhs.Size_;
        Capacity_ = rhs.Capacity_;
        rhs.Begin_ = nullptr;
        rhs.Size_ = rhs.Capacity_ = 0;
    }
    return *this;
}

void TBlob::Append(const void* data, size_t size)
{
    Resize(Size_ + size, false);
    memcpy(Begin_ + Size_ - size, data, size);
}

void TBlob::Append(const TRef& ref)
{
    Append(ref.Begin(), ref.Size());
}

void TBlob::Reset()
{
    Begin_ = nullptr;   
    Size_ = Capacity_ = 0;
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef::TSharedData::TSharedData(TBlob&& blob)
    : Blob(std::move(blob))
#ifdef ENABLE_REF_COUNTED_TRACKING
    , Cookie(nullptr)
#endif
{ }

TSharedRef::TSharedData::~TSharedData()
{
#ifdef ENABLE_REF_COUNTED_TRACKING
    FinalizeTracking();
#endif
}

#ifdef ENABLE_REF_COUNTED_TRACKING

void TSharedRef::TSharedData::InitializeTracking(void* cookie)
{
    YASSERT(!Cookie);
    Cookie = cookie;
    TRefCountedTracker::Get()->Allocate(Cookie, Blob.Size());
}

void TSharedRef::TSharedData::FinalizeTracking()
{
    YASSERT(Cookie);
    TRefCountedTracker::Get()->Free(Cookie, Blob.Size());
}

#endif

////////////////////////////////////////////////////////////////////////////////

TSharedRef TSharedRef::AllocateImpl(size_t size, bool initializeStorage)
{
    TBlob blob(size, initializeStorage);
    return FromBlobImpl(std::move(blob));
}

TSharedRef TSharedRef::FromBlobImpl(TBlob&& blob)
{
    auto ref = TRef::FromBlob(blob);
    auto data = New<TSharedData>(std::move(blob));
    return TSharedRef(data, ref);
}

Stroka ToString(const TRef& ref)
{
    return Stroka(ref.Begin(), ref.End());
}

void Save(TOutputStream* output, const NYT::TSharedRef& ref)
{
    if (ref == TSharedRef()) {
        ::Save(output, static_cast<i64>(-1));
    } else {
        ::Save(output, static_cast<i64>(ref.Size()));
        output->Write(ref.Begin(), ref.Size());
    }
}

void Load(TInputStream* input, NYT::TSharedRef& ref)
{
    i64 size;
    ::Load(input, size);
    if (size == -1) {
        ref = NYT::TSharedRef();
    } else {
        YASSERT(size >= 0);
        struct TLoadedBlockTag { };
        ref = TSharedRef::Allocate<TLoadedBlockTag>(size);
        YCHECK(input->Load(ref.Begin(), size) == size);
    }
}

////////////////////////////////////////////////////////////////////////////////

size_t RoundUpToPage(size_t bytes)
{
    static const size_t PageSize = NSystemInfo::GetPageSize();
    YASSERT((PageSize & (PageSize - 1)) == 0);
    return (bytes + PageSize - 1) & (~(PageSize - 1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
