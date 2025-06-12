#pragma once

#include <library/cpp/yt/memory/ref.h>

#include <util/generic/fwd.h>

#include <util/stream/input.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/interfaces.h>

// TODO(achulkov2): Rename this library arrow_parquet_adapter -> arrow_adapter. Maybe separate into different files.

namespace NYT::NArrow {

////////////////////////////////////////////////////////////////////////////////

void ThrowOnError(const arrow::Status& status);

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow::io::RandomAccessFile> CreateParquetAdapter(
    const TString* metadata,
    i64 startMetadataOffset,
    std::shared_ptr<IInputStream> reader = nullptr);

std::shared_ptr<arrow::Schema> CreateArrowSchemaFromParquetMetadata(const TString* metadata, i64 startIndex);

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
    : public arrow::io::RandomAccessFile
{
public:
    //! Descendants of this class must implement the two methods below.
    arrow::Result<int64_t> GetSize() override = 0;
    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override = 0;

    //! Implemented via the ReadAt method above.
    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;
    
    //! The methods below are implemented as throwing stubs, only override them if you need to.
    arrow::Status Seek(int64_t /*position*/) override;
    arrow::Result<int64_t> Tell() const override;

    //! The methods below are implemented as throwing stubs, only override them if you need to.
    arrow::Result<int64_t> Read(int64_t /*nbytes*/, void* /*out*/) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t /*nbytes*/) override;

    //! These methods have legit implementations. Override them if you need to.
    arrow::Status Close() override;
    bool closed() const override;

protected:
    //! Thread-safety is not mandated by arrow, but let's be safe.
    std::atomic<bool> Closed_ = false;
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

    arrow::Result<int64_t> GetSize() override;

    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;

private:
    std::vector<TBufferDescriptor> Buffers_;
    i64 FileSize_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
