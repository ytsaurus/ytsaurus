#pragma once

#include <util/generic/vector.h>
#include <util/generic/ptr.h>

#include <mapreduce/yt/interface/io.h>

#include "node_table_reader.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNodeYdlTableReader
    : public IYdlReaderImpl
{
public:
    TNodeYdlTableReader(
        ::TIntrusivePtr<TRawTableReader> input,
        TVector<ui64> hashes);

    const TNode& GetRow() const override;
    void VerifyRowType(ui64 rowTypeHash) const override;

    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui32 GetRangeIndex() const override;
    ui64 GetRowIndex() const override;
    void NextKey() override;
    TMaybe<size_t> GetReadByteCount() const override;
    bool IsEndOfStream() const override;
    bool IsRawReaderExhausted() const override;

private:
    THolder<TNodeTableReader> NodeReader_;
    TVector<ui64> TypeHashes_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
