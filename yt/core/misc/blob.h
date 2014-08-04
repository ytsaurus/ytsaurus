#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Forward declarations.
class TRef;

////////////////////////////////////////////////////////////////////////////////

//! A home-grown optimized replacement for |std::vector<char>| suitable for carrying
//! large chunks of data.
/*!
 *  Compared to |std::vector<char>|, this class supports uninitialized allocations
 *  when explicitly requested to.
 */
// TODO(babenko): integrate with Ref Counted Tracker.
class TBlob
{
public:
    //! Constructs an empty blob.
    TBlob();

    //! Constructs a blob with a given size.
    explicit TBlob(size_t size, bool initiailizeStorage = true);

    //! Copies the data.
    TBlob(const TBlob& other);

    //! Moves the data (takes the ownership).
    TBlob(TBlob&& other);

    //! Copies a chunk of memory into a new instance.
    TBlob(const void* data, size_t size);

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

    void Reset();

};

void swap(TBlob& left, TBlob& right);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

