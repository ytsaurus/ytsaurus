#pragma once

#include "public.h"
#include "medium_directory.h"

#include <yt/yt/library/s3/client.h>
#include <yt/yt/library/arrow_parquet_adapter/arrow.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////

class TS3ArrowRandomAccessFile
    : public NArrow::TStatlessArrowRandomAccessFileBase
{
public:
    //! NB: Constructor performs a context switch to fetch file size.
    TS3ArrowRandomAccessFile(
        TString bucket,
        TString key,
        NS3::IClientPtr client);

    arrow::Result<int64_t> GetSize() override;

    //! NB: Context switch.
    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;

private:
    const TString Bucket_;
    const TString Key_;
    const NS3::IClientPtr Client_;

    i64 FileSize_;

    //! Fetches the file size from S3 via HeadObject request.
    TFuture<i64> FetchFileSize();
};

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient