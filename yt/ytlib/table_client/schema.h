#pragma once

#include "common.h"
#include "value.h"
#include "table_chunk_meta.pb.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef Stroka TColumn;

////////////////////////////////////////////////////////////////////////////////

//! Range of columns used as a part of channel description.
class TRange
{
public:
    TRange(const TColumn& begin, const TColumn& end);

    //! Creates infinite range.
    TRange(const TColumn& begin);

    TColumn Begin() const;
    TColumn End() const;

    NProto::TRange ToProto() const;
    static TRange FromProto(const NProto::TRange& protoRange);

    bool Contains(const TColumn& value) const;
    bool Contains(const TRange& range) const;
    bool Overlaps(const TRange& range) const;

    bool IsInfinite() const;

private:
    bool IsInfinite_;
    TColumn Begin_;
    TColumn End_;
};

////////////////////////////////////////////////////////////////////////////////

//! Part of schema descriptions. Set of fixed columns and column ranges.
class TChannel
{
public:
    void AddColumn(const TColumn& column);
    void AddRange(const TRange& range);
    void AddRange(const TColumn& begin, const TColumn& end);

    bool Contains(const TColumn& column) const;
    bool Contains(const TChannel& channel) const;
    bool Contains(const TRange& range) const;
    bool ContainsInRanges(const TColumn& column) const;

    bool Overlaps(const TChannel& channel) const;
    bool Overlaps(const TRange& range) const;

    bool IsEmpty() const;

    NProto::TChannel ToProto() const;
    static TChannel FromProto(const NProto::TChannel& protoChannel);

    const yvector<TColumn>& GetColumns() const;

    //! Returns a channel containing range from empty string to infinity.
    static TChannel Universal();

private:
    friend void operator -= (TChannel& lhs, const TChannel& rhs);

    yvector<TColumn> Columns;
    yvector<TRange> Ranges;
};

////////////////////////////////////////////////////////////////////////////////

class TSchema
{
public:
    TSchema();
    void AddChannel(const TChannel& channel);
    const yvector<TChannel>& GetChannels() const;

private:
    yvector<TChannel> Channels;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
