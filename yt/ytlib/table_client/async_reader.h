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
    
    virtual bool HasRow() const = 0;
    virtual const TRow& GetRow() const = 0;
    virtual const TNonOwningKey& GetKey() const = 0;
    virtual const NYTree::TYsonString& GetRowAttributes() const = 0;

    virtual i64 GetRowIndex() const = 0;
    virtual i64 GetRowCount() const = 0;
    virtual std::vector<NChunkClient::TChunkId> GetFailedChunks() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
