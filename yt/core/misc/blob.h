#pragma once

#include "common.h"
#include "ref_counted.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Forward declarations.
class TRef;

////////////////////////////////////////////////////////////////////////////////

//! Default memory tag for TBlob.
struct TDefaultBlobTag { };

//! A home-grown optimized replacement for |std::vector<char>| suitable for carrying
//! large chunks of data.
/*!
 *  Compared to |std::vector<char>|, this class supports uninitialized allocations
 *  when explicitly requested to.
 */
class TBlob
{
public:
    //! Constructs a blob with a given size.
    TBlob(TRefCountedTypeCookie tagCookie, size_t size, bool initiailizeStorage);

    //! Copies a chunk of memory into a new instance.
    TBlob(TRefCountedTypeCookie tagCookie, const void* data, size_t size);

    //! Constructs an empty blob.
    template <class TTag = TDefaultBlobTag>
    explicit TBlob(TTag tag = TTag())
        : TBlob(tag, 0)
    { }

    //! Constructs a blob with a given size.
    template <class TTag>
    explicit TBlob(TTag tag, size_t size, bool initiailizeStorage = true)
        : TBlob(GetRefCountedTypeCookie<TTag>(), size, initiailizeStorage)
    { }

    //! Copies a chunk of memory into a new instance.
    template <class TTag>
    TBlob(TTag tag, const void* data, size_t size)
        : TBlob(GetRefCountedTypeCookie<TTag>(), data, size)
    { }

    //! Remind user about the tag argument.
    TBlob(i32 size, bool initiailizeStorage = true) = delete;
    TBlob(i64 size, bool initiailizeStorage = true) = delete;
    TBlob(ui32 size, bool initiailizeStorage = true) = delete;
    TBlob(ui64 size, bool initiailizeStorage = true) = delete;
    template <typename T, typename U> TBlob(const T*, U) = delete;

    //! Copies the data.
    TBlob(const TBlob& other);

    //! Moves the data (takes the ownership).
    TBlob(TBlob&& other);

    //! Reclaims the memory.
    ~TBlob();

    //! Ensures that capacity is at least #capacity.
    void Reserve(size_t newCapacity);

    //! Changes the size to #newSize.
    /*!
     *  If #size exceeds the current capacity,
     *  we make sure the new capacity grows exponentially.
     *  Hence calling #Resize N times to increase the size by N only
     *  takes amortized O(1) time per call.
     */
    void Resize(size_t newSize, bool initializeStorage = true);

    //! Returns the start pointer.
    FORCED_INLINE const char* Begin() const
    {
        return Begin_;
    }

    //! Returns the start pointer.
    FORCED_INLINE char* Begin()
    {
        return Begin_;
    }

    //! Returns the end pointer.
    FORCED_INLINE const char* End() const
    {
        return Begin_ + Size_;
    }

    //! Returns the end pointer.
    FORCED_INLINE char* End()
    {
        return Begin_ + Size_;
    }

    //! Returns the size.
    FORCED_INLINE size_t Size() const
    {
        return Size_;
    }

    //! Returns the capacity.
    FORCED_INLINE size_t Capacity() const
    {
        return Capacity_;
    }

    //! Returns the TStringBuf instance for the occupied part of the blob.
    FORCED_INLINE TStringBuf ToStringBuf() const
    {
        return TStringBuf(Begin_, Size_);
    }

    //! Provides by-value access to the underlying storage.
    FORCED_INLINE char operator [] (size_t index) const
    {
        return Begin_[index];
    }

    //! Provides by-ref access to the underlying storage.
    FORCED_INLINE char& operator [] (size_t index)
    {
        return Begin_[index];
    }

    //! Clears the instance but does not reclaim the memory.
    FORCED_INLINE void Clear()
    {
        Size_ = 0;
    }

    //! Returns |true| if size is zero.
    FORCED_INLINE bool IsEmpty() const
    {
        return Size_ == 0;
    }

    //! Overwrites the current instance.
    TBlob& operator = (const TBlob& rhs);

    //! Takes the ownership.
    TBlob& operator = (TBlob&& rhs);

    //! Appends a chunk of memory to the end.
    void Append(const void* data, size_t size);

    //! Appends a chunk of memory to the end.
    void Append(const TRef& ref);

    //! Appends a single char to the end.
    void Append(char ch);

    //! Swaps the current and other instances
    void Swap(TBlob& other);

    friend void swap(TBlob& left, TBlob& right);

private:
    char* Begin_;
    size_t Size_;
    size_t Capacity_;

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TRefCountedTypeCookie TypeCookie_ = NullRefCountedTypeCookie;
#endif

    void Allocate(size_t newCapacity);
    void Reallocate(size_t newCapacity);
    void Free();

    void Reset();

    void SetTypeCookie(TRefCountedTypeCookie tagCookie);
    void SetTypeCookie(const TBlob& other);
};

void swap(TBlob& left, TBlob& right);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

