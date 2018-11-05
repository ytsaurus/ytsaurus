#pragma once
#ifndef PARTITION_CHUNK_READER_INL_H_
#error "Direct inclusion of this file is not allowed, include partition_chunk_reader.h"
// For the sake of sane code completion.
#include "partition_chunk_reader.h"
#endif

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TValueInsertIterator, class TRowDescriptorInsertIterator>
bool TPartitionChunkReader::Read(
    TValueInsertIterator& valueInserter,
    TRowDescriptorInsertIterator& rowDescriptorInserter,
    i64* rowCount)
{
    *rowCount = 0;

    if (!BeginRead()) {
        // Not ready yet.
        return true;
    }

    if (!BlockReader_) {
        // Nothing to read from chunk.
        return false;
    }

    if (BlockEnded_) {
        BlockReader_ = nullptr;
        return OnBlockEnded();
    }

    while (true) {
        ++(*rowCount);

        const auto& key = BlockReader_->GetKey();

        std::copy(key.Begin(), key.End(), valueInserter);
        rowDescriptorInserter = TRowDescriptor{
            BlockReader_,
            static_cast<i32>(BlockReader_->GetRowIndex())};

        if (!BlockReader_->NextRow()) {
            BlockEnded_ = true;
            return true;
        }
    }

    return true;
}

template <class TValueInsertIterator, class TRowDescriptorInsertIterator>
bool TPartitionMultiChunkReader::Read(
    TValueInsertIterator& valueInserter,
    TRowDescriptorInsertIterator& rowDescriptorInserter,
    i64* rowCount)
{
    if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
        return true;
    }

    *rowCount = 0;

    // Nothing to read.
    if (!CurrentReader_) {
        return false;
    }

    bool readerFinished = !CurrentReader_->Read(valueInserter, rowDescriptorInserter, rowCount);
    if (*rowCount == 0) {
        return TParallelMultiReaderBase::OnEmptyRead(readerFinished);
    } else {
        return true;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
