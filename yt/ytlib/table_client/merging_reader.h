#pragma once

#include "public.h"
#include "sync_reader.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TMergingReader
    : public ISyncReader
{
public:
    TMergingReader(const std::vector<TChunkSequenceReaderPtr>& readers);

    virtual void Open();
    virtual void NextRow();

    virtual bool IsValid() const;
    virtual TRow& GetRow();
    const TNonOwningKey& GetKey() const;

    virtual const NYTree::TYson& GetRowAttributes() const;

private:
    std::vector<TChunkSequenceReaderPtr> Readers;
    std::vector<TChunkSequenceReader*> ReaderHeap;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
