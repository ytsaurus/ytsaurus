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
        Capacity_ = std::max(size, InitialBlobCapacity);
        Begin_ = new char[Capacity_];
        Size_ = size;
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
        Capacity_ = std::max(InitialBlobCapacity, other.Size_);
        Begin_ = new char[Capacity_];
        memcpy(Begin_, other.Begin_, other.Size_);
        Size_ = other.Size_;
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
        if (rhs.Size_ == 0) {
            Reset();
        } else {
            Capacity_ = std::max(InitialBlobCapacity, rhs.Size_);
            Begin_ = new char[Capacity_];
            memcpy(Begin_, rhs.Begin_, rhs.Size_);
            Size_ = rhs.Size_;
        }
    }
    return *this;
}

TBlob& TBlob::operator = (TBlob&& rhs)
{
    if (this != &rhs) {
        delete[] Begin_;
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

void swap(TBlob& left, TBlob& right)
{
    if (&left != &right) {
        std::swap(left.Begin_, right.Begin_);
        std::swap(left.Size_, right.Size_);
        std::swap(left.Capacity_, right.Capacity_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
