#include "../misc/stdafx.h"
#include "io_channels.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TReadChannel::TReadChannel()
    : NextBlockIdx(0)
    , RowCount(0)
    , RowIdx(0)
{ }

TReadChannel::~TReadChannel()
{}

void TReadChannel::ReadRow()
{
    Values.clear();
    Columns.clear();

    // alone columns
    for (size_t a = 0; a < AloneCount; ++a) {
        if (ColumnIdxToUserIdx.find(a) != ColumnIdxToUserIdx.end()) {
            Buffer.SetReadPos(AlonePos[a]);
            TValue value = Buffer.ReadValue();
            if (!value.IsNull()) {
                Values.push_back(value);
                Columns.push_back(a);
            }
            AlonePos[a] = Buffer.GetReadPos();
        }
    }

    // range columns
    Buffer.SetReadPos(RangePos);
    TValue value = Buffer.ReadValue();
    while (!value.IsNull()) {
        size_t rangeIdx = Buffer.ReadNumber() + AloneCount;
        if (ColumnIdxToUserIdx.find(rangeIdx) != ColumnIdxToUserIdx.end()) {
            Values.push_back(value);
            Columns.push_back(rangeIdx);
        }
        value = Buffer.ReadValue();
    }
    RangePos = Buffer.GetReadPos();

    ColumnIdx = 0;
}

// User (filtering) channel, not chunk channel
void TReadChannel::SetChannel(const TChannel& channel)
{
    Channel = channel;
}

TValue TReadChannel::GetValue()
{
    return Values[ColumnIdx];
}

size_t TReadChannel::GetIndex()
{
    return ColumnIdxToUserIdx[Columns[ColumnIdx]];
}

void TReadChannel::NextRow()
{
    if (RowIdx < RowCount) {
        ReadRow();
        ++RowIdx;
    } else
        Buffer.Clear();
}

void TReadChannel::NextColumn()
{
    ++ColumnIdx;
}

bool TReadChannel::EndColumn() const
{
    YASSERT(ColumnIdx <= Columns.size());
    return ColumnIdx == Columns.size();
}

bool TReadChannel::NeedsBlock() const
{
    YASSERT(RowIdx <= RowCount);
    return RowIdx == RowCount;
}

size_t TReadChannel::GetNextBlockIdx() const
{
    return NextBlockIdx;
}

void TReadChannel::SetBlock(TBlob* data, TUserIndexes& userIndexes)
{
    ++NextBlockIdx;
    ColumnIdxToUserIdx.clear();

    Buffer.SetData(data);

    // version
    size_t version = Buffer.ReadNumber();
    YASSERT(version == 1);

    // row count
    RowCount = Buffer.ReadNumber();

    // alone column names
    // TODO: parse alone columns once per chunk from channel description in header
    AloneCount = Buffer.ReadNumber();
    for (size_t c = 0; c < AloneCount; ++c) {
        TValue aloneName = Buffer.ReadValue();
        if (Channel.Match(aloneName))
            ColumnIdxToUserIdx[c] = userIndexes[aloneName];
    }
    
    // range column names
    RangeCount = Buffer.ReadNumber();
    for (size_t c = 0; c < RangeCount; ++c) {
        TValue rangeName = Buffer.ReadValue();
        // ReadChannel range column idx = block range col idx + AloneCount
        if (Channel.Match(rangeName))
            ColumnIdxToUserIdx[AloneCount + c] = userIndexes[rangeName];
    }

    // column offsets
    AlonePos.yresize(AloneCount);
    ui32 pos;
    for (size_t c = 0; c < AloneCount; ++c) {
        AlonePos[c] = pos;
        pos += Buffer.ReadNumber();
    }
    RangePos = pos;

    for (size_t c = 0; c < AloneCount; ++c) {
        AlonePos[c] += Buffer.GetReadPos();
    }
    RangePos += Buffer.GetReadPos();

    RowIdx = 1;
    ReadRow();
}


////////////////////////////////////////////////////////////////////////////////

TWriteChannel::TWriteChannel()
{}

TWriteChannel::~TWriteChannel()
{}

void TWriteChannel::SetChannel(const TChannel& channel)
{
    Channel = channel;

    AloneCount = Channel.Columns.size();
    AloneNamesSize = 0;
    for (size_t c = 0; c < AloneCount; ++c)
        AloneNamesSize += Channel.Columns[c].GetSize();

    AloneSizes.resize(AloneCount);
    RowAlonePos.resize(AloneCount);
}

void TWriteChannel::SetBlockSizeLimit(ui32 blockSizeLimit)
{
    BlockSizeLimit = blockSizeLimit;
    Buffer.SetSizeLimit(BlockSizeLimit);
}

void TWriteChannel::SetColumnIndex(size_t userIdx, const TValue& column)
{
    for (size_t c = 0; c < Channel.Columns.size(); ++c) {
        if (Channel.Columns[c] == column) {
            UserIdxToAloneIdx[userIdx] = c;
            return;
        }
    }

    // Range column
    UserIdxToRangeSize[userIdx] = column.GetSize();
}

void TWriteChannel::Set(size_t userIdx, const TValue& value)
{
    ui32 pos = Buffer.GetWritePos() + 1;

    auto i = UserIdxToAloneIdx.find(userIdx);
    if (i != UserIdxToAloneIdx.end()) {
        size_t aloneIdx = i->second;
        size_t size = Buffer.WriteValue(value);
        RowAlonePos[aloneIdx] = pos;
        AloneSizes[aloneIdx] += size;
    } else {
        if (!RangeColumns.has(userIdx)) {
            YASSERT(UserIdxToRangeSize[userIdx] > 0);

            RangeColumns.insert(userIdx);
            RangeNamesSize += UserIdxToRangeSize[userIdx];
        }
        size_t size = Buffer.WriteValue(value);
        size += Buffer.WriteNumber(userIdx);
        RowRangePos.push_back(pos);
        RangeSize += size;
    }
}

void TWriteChannel::StartRow()
{
    for (size_t c = 0; c < AloneCount; ++c)
        RowAlonePos[c] = 0;
    RowRangePos.yresize(0);
}

void TWriteChannel::StartBlock()
{
    RangeColumns.clear();
    RangeNamesSize = 0;

    AlonePos.yresize(0);
    RangePos.yresize(0);
    for (size_t c = 0; c < AloneCount; ++c)
        AloneSizes[c] = 0;
    RangeSize = 0;

    RowCount = 0;
    Buffer.Clear();

    StartRow();
}

void TWriteChannel::AddRow()
{
    for (size_t c = 0; c < AloneCount; ++c) {
        if (!RowAlonePos[c])
            AloneSizes[c] += Buffer.WriteNumber(0);
    }
    RangeSize += Buffer.WriteNumber(0);

    AlonePos.insert(AlonePos.end(), RowAlonePos.begin(), RowAlonePos.end());
    RangePos.insert(RangePos.end(), RowRangePos.begin(), RowRangePos.end());
    RangePos.push_back(0);

    ++RowCount;

    StartRow();
}

size_t TWriteChannel::GetBlockSize() const
{
    return Buffer.GetWritePos() + AloneNamesSize + RangeNamesSize;
}

bool TWriteChannel::HasBlock() const
{
    return GetBlockSize() >= BlockSizeLimit;
}

void TWriteChannel::GetBlock(TBlob *data, TUserIndexes& userIndexes)
{
    TBlockBuffer buffer;
    buffer.SetSizeLimit(BlockSizeLimit + 1024); // CRAP
    buffer.Clear();

    // version
    buffer.WriteNumber(1);

    // row count
    buffer.WriteNumber(RowCount);

    // alone column names
    buffer.WriteNumber(AloneCount);
    for (size_t c = 0; c < AloneCount; ++c)
        buffer.WriteValue(Channel.Columns[c]);
    
    // range column names
    size_t rangeCount = RangeColumns.size();
    buffer.WriteNumber(rangeCount);

    yhash_map<size_t, size_t> userIdxToRangeIdx;
    auto i = RangeColumns.begin();
    for (size_t c = 0; c < rangeCount; ++c, ++i) {
        buffer.WriteValue(userIndexes[*i]);
        userIdxToRangeIdx[*i] = c;
    }

    // alone column sizes
    for (size_t c = 0; c < AloneCount; ++c)
        buffer.WriteNumber(AloneSizes[c]);

    // alone columns
    for (size_t c = 0; c < AloneCount; ++c) {
        size_t check = buffer.GetWritePos();

        size_t idx = c;
        for (size_t r = 0; r < RowCount; ++r, idx += AloneCount) {
            ui32 pos = AlonePos[idx];
            if (pos) { // value is set
                Buffer.SetReadPos(pos - 1);
                buffer.WriteValue(Buffer.ReadValue());
            } else
                buffer.WriteNumber(0);
        }

        YASSERT(buffer.GetWritePos() - check == AloneSizes[c]);
    }

    // range column
    for (size_t r = 0, idx = 0; r < RowCount; ++r) {
        ui32 pos = 0;
        for (; pos = RangePos[idx]; ++idx) {
            Buffer.SetReadPos(pos - 1);
            buffer.WriteValue(Buffer.ReadValue());
            size_t userIdx = Buffer.ReadNumber();
            YASSERT(userIdxToRangeIdx.find(userIdx) != userIdxToRangeIdx.end());
            buffer.WriteNumber(userIdxToRangeIdx[userIdx]);
        }
        buffer.WriteNumber(0);
        ++idx;
    }

    buffer.GetData(data);
}

////////////////////////////////////////////////////////////////////////////////

}
