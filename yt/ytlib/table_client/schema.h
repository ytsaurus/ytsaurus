#pragma once

#include "common.h"
#include "value.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef Stroka TColumn;

////////////////////////////////////////////////////////////////////////////////

class TRange
{
public:
    TRange(TColumn begin, TColumn end);

    //! Creates open range
    TRange(TColumn begin);

    TColumn Begin() const;
    TColumn End() const;

    void FillProto(NProto::TRange* protoRange) const;

    bool Contains(TColumn value) const;
    bool Overlaps(const TRange& range) const;

    //! True if range is open
    bool IsOpen() const;

private:
    TColumn Begin_;
    TColumn End_;
};

////////////////////////////////////////////////////////////////////////////////

// Part of schema descriptions
// Set of fixed columns and column ranges
class TChannel
{
public:
    TChannel& AddColumn(TColumn column);
    TChannel& AddRange(const TRange& range);
    TChannel& AddRange(TColumn begin, TColumn end);

    bool Contains(TColumn column) const;
    bool ContainsInRanges(TColumn column) const;

    void FillProto(NProto::TChannel* protoChannel) const;

    const yvector<TColumn>& Columns();

    TChannel& operator-= (const TChannel& channel);
    const TChannel operator- (const TChannel& channel);

private:
    yvector<TColumn> Columns_;
    yvector<TRange> Ranges_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchema
{
public:
    TSchema();
    TSchema& AddChannel(const TChannel& channel);
    const yvector<TChannel>& Channels() const;

private:
    yvector<TChannel> Channels_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
