#pragma once

#include <library/cpp/yt/memory/ref.h>

#include <util/generic/fwd.h>

#include <util/stream/input.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/interfaces.h>

// TODO(achulkov2): Rename this library arrow_parquet_adapter -> arrow_adapter. Maybe separate into different files.

namespace NYT::NArrow {

////////////////////////////////////////////////////////////////////////////////

void ThrowOnError(const arrow20::Status& status);

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow20::io::RandomAccessFile> CreateParquetAdapter(
    const TString* metadata,
    i64 startMetadataOffset,
    std::shared_ptr<IInputStream> reader = nullptr);

std::shared_ptr<arrow20::Schema> CreateArrowSchemaFromParquetMetadata(const TString* metadata, i64 startIndex);

////////////////////////////////////////////////////////////////////////////////

//! The random access file interface represents a stateful positioned reader, with
//! a default random-access ReadAt method, implemented via lock + seek + read.
//! In practice, arrow libraries use the ReadAt family of methods when working with
//! random access files, and for most custom implementations, these methods can be
//! implemented more effieciently and without locks.
//! Thus, this base class overrides all stateful position-related methods with error-returning
//! stubs, and overrides the closing logic with an elementary implementation.
//!
//! Descendants of this class should override GetSize and a single ReadAt method.
class TStatlessArrowRandomAccessFileBase
    : public arrow20::io::RandomAccessFile
{
public:
    //! Descendants of this class must implement the two methods below.
    arrow20::Result<int64_t> GetSize() override = 0;
    arrow20::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override = 0;

    //! Implemented via the ReadAt method above.
    arrow20::Result<std::shared_ptr<arrow20::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;
    
    arrow20::Status Seek(int64_t /*position*/) override;
    arrow20::Result<int64_t> Tell() const override;

    arrow20::Result<int64_t> Read(int64_t /*nbytes*/, void* /*out*/) override;
    arrow20::Result<std::shared_ptr<arrow20::Buffer>> Read(int64_t /*nbytes*/) override;

    //! These methods have legit implementations. Override them if you need to.
    arrow20::Status Close() override;
    bool closed() const override;

protected:
    //! Thread-safety is not mandated by arrow, but let's be safe.
    std::atomic<bool> Closed_ = false;
    i64 FilePosition_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Implements a random access file interface that reads from a set of buffers.
//! The buffers are not required to represent a contiguous file.
//! If an attempt is made to read outside the range of the buffers, an error is returned.
class TCompositeBufferArrowRandomAccessFile
    : public TStatlessArrowRandomAccessFileBase
{
public:
    struct TBufferDescriptor
    {
        TSharedRef Data;
        i64 Offset;
    };

    TCompositeBufferArrowRandomAccessFile(const std::vector<TBufferDescriptor>& buffers, i64 fileSize);

    arrow20::Result<int64_t> GetSize() override;

    arrow20::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;

private:
    std::vector<TBufferDescriptor> Buffers_;
    i64 FileSize_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
