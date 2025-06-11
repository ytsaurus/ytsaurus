#include "s3_common.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>


namespace NYT::NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////

TS3ArrowRandomAccessFile::TS3ArrowRandomAccessFile(
    TString bucket,
    TString key,
    NS3::IClientPtr client)
    : Bucket_(std::move(bucket))
    , Key_(std::move(key))
    , Client_(std::move(client))
{
    auto fileSize = WaitFor(FetchFileSize())
        .ValueOrThrow();
    
    FileSize_ = fileSize;
}

arrow::Result<int64_t> TS3ArrowRandomAccessFile::GetSize()
{
    return FileSize_;
}

arrow::Result<int64_t> TS3ArrowRandomAccessFile::ReadAt(int64_t position, int64_t nbytes, void* out)
{
    if (position < 0 || position > FileSize_) {
        return arrow::Status::Invalid(Format("Read position %v is out of file bounds [0, %v)", position, FileSize_));
    }

    nbytes = std::min(nbytes, FileSize_ - position);

    NS3::TGetObjectRequest request;
    request.Bucket = Bucket_;
    request.Key = Key_;
    request.Range = Format("bytes=%v-%v", position, position + nbytes - 1);

    auto response = WaitFor(Client_->GetObject(request))
        .ValueOrThrow();

    std::memcpy(out, response.Data.Begin(), response.Data.Size());
    return response.Data.Size();
}

TFuture<i64> TS3ArrowRandomAccessFile::FetchFileSize()
{
    NS3::THeadObjectRequest request;
    request.Bucket = Bucket_;
    request.Key = Key_;

    return Client_->HeadObject(request)
        .Apply(BIND([] (const NS3::THeadObjectResponse& response) {
            return response.Size;
        }));
}

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient