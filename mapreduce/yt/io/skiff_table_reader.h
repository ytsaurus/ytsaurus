#pragma once

#include "counting_raw_reader.h"

#include <mapreduce/yt/interface/io.h>

#include <mapreduce/yt/skiff/public.h>
#include <mapreduce/yt/skiff/wire_type.h>
#include <mapreduce/yt/skiff/unchecked_parser.h>

#include <util/stream/buffered.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TSkiffTableReader
    : public INodeReaderImpl
{
public:
    TSkiffTableReader(
        ::TIntrusivePtr<TRawTableReader> input,
        const NSkiff::TSkiffSchemaPtr& schema);
    ~TSkiffTableReader() override;

    virtual const TNode& GetRow() const override;
    virtual void MoveRow(TNode* row) override;

    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui32 GetRangeIndex() const override;
    ui64 GetRowIndex() const override;
    void NextKey() override;
    TMaybe<size_t> GetReadByteCount() const override;
    bool IsRawReaderExhausted() const override;

private:
    struct TSkiffTableSchema;

private:
    void EnsureValidity() const;
    void ReadRow();
    static TVector<TSkiffTableSchema> CreateSkiffTableSchemas(const NSkiff::TSkiffSchemaPtr& schema);

private:
    NDetail::TCountingRawTableReader Input_;
    TBufferedInput BufferedInput_;
    NSkiff::TUncheckedSkiffParser Parser_;
    TVector<TSkiffTableSchema> Schemas_;

    TNode Row_;

    bool Valid_ = true;
    bool AfterKeySwitch_ = false;
    bool Finished_ = false;
    TMaybe<ui64> RangeIndex_;
    TMaybe<ui64> RowIndex_;
    ui32 TableIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
