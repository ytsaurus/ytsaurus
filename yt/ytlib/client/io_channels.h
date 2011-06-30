#pragma once

#include "types.h"
#include "block_buffer.h"
#include "user_indexes.h"

namespace NYT {
    
////////////////////////////////////////////////////////////////////////////////

class TReadChannel
{
    TChannel Channel; // Channel of parent TRead - filtering channel

    TBlockBuffer Buffer;
    size_t NextBlockIdx; // index of next block in current chunk's channel

    size_t RowCount; // current block's row count
    size_t RowIdx;

    // column index -> user column index
    yhash_map<size_t, size_t> ColumnIdxToUserIdx;

    size_t AloneCount;
    size_t RangeCount;

    yvector<ui32> AlonePos;
    ui32 RangePos;

    yvector<TValue> Values;
    yvector<size_t> Columns;

    size_t ColumnIdx;

    void ReadRow();
public:

    TReadChannel();
    ~TReadChannel();

    void SetChannel(const TChannel& channel);    

    TValue GetValue();
    size_t GetIndex(); // UserIndex

    void NextRow();
    void NextColumn();
    bool EndColumn() const;

    bool NeedsBlock() const;
    size_t GetNextBlockIdx() const;
    void SetBlock(TBlob* data, TUserIndexes& userIndexes);
};

////////////////////////////////////////////////////////////////////////////////

class TWriteChannel
{
    TChannel Channel;

    TBlockBuffer Buffer;
    ui32 BlockSizeLimit; // target block size

    size_t RowCount;

    // user column index -> alone column index + 1, 0 if range
    yhash_map<size_t, size_t> UserIdxToAloneIdx;
    yhash_map<size_t, size_t> UserIdxToRangeSize;
    yhash_set<size_t> RangeColumns;

    size_t AloneCount; // number of stand-alone columns, constant

    size_t AloneNamesSize; // sum of sizes of alone column names, constant
    size_t RangeNamesSize; // current sum of sizes of range column names

    yvector<size_t> AloneSizes; // current sizes of alone columns
    size_t RangeSize; // current size of range column

    yvector<ui32> AlonePos; // shifts of alone columns
    yvector<ui32> RangePos; // shifts of range columns

    yvector<ui32> RowAlonePos; // shifts of alone columns, current row
    yvector<ui32> RowRangePos; // shifts of range columns, current row

public:

    TWriteChannel();
    ~TWriteChannel();

    void SetChannel(const TChannel& channel);
    void SetBlockSizeLimit(ui32 blockSizeLimit);

    void SetColumnIndex(size_t userIdx, const TValue& column);

    void Set(size_t userIdx, const TValue& value);
    void StartRow();
    void StartBlock();
    void AddRow();

    size_t GetBlockSize() const;
    bool HasBlock() const;
    void GetBlock(TBlob *data, TUserIndexes& userIndexes);
};

////////////////////////////////////////////////////////////////////////////////

}
