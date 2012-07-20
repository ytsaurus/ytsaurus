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
    TMergingReader(const std::vector<TTableChunkSequenceReaderPtr>& readers);

    virtual void Open();
    virtual void NextRow();

    virtual bool IsValid() const;
    virtual const TRow& GetRow();
    const TNonOwningKey& GetKey() const;

private:
    std::vector<TTableChunkSequenceReaderPtr> Readers;
    std::vector<TTableChunkSequenceReader*> ReaderHeap;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
