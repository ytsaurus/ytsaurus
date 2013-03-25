#include "stdafx.h"
#include "blob.h"
#include "ref.h"

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

} // namespace NYT
