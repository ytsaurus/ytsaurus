#pragma once

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IAsyncReader
    : public virtual TRefCounted
{
    virtual TAsyncError AsyncOpen() = 0;

    virtual bool FetchNextItem() = 0;
    virtual TAsyncError GetReadyEvent() = 0;

    virtual bool IsValid() const = 0;
    virtual const TRow& GetRow() const = 0;
    virtual int GetTableIndex() const = 0;

    virtual i64 GetSessionRowIndex() const = 0;
    virtual i64 GetSessionRowCount() const = 0;
    virtual i64 GetTableRowIndex() const = 0;
    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
