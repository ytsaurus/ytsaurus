#pragma once

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/ytree/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Range of columns used as a part of channel description.
class TRange
{
public:
    TRange(const Stroka& begin, const Stroka& end);

    //! Creates infinite range.
    explicit TRange(const Stroka& begin);

    Stroka Begin() const;
    Stroka End() const;

    NProto::TRange ToProto() const;
    static TRange FromProto(const NProto::TRange& protoRange);

    bool Contains(const TStringBuf& value) const;
    bool Contains(const TRange& range) const;
    bool Overlaps(const TRange& range) const;

    bool IsInfinite() const;

private:
    bool IsInfinite_;
    Stroka Begin_;
    Stroka End_;
};

////////////////////////////////////////////////////////////////////////////////

//! Set of fixed columns and column ranges.
class TChannel
{
public:
    void AddColumn(const Stroka& column);
    void AddRange(const TRange& range);
    void AddRange(const Stroka& begin, const Stroka& end);

    bool Contains(const TStringBuf& column) const;
    bool Contains(const TChannel& channel) const;
    bool Contains(const TRange& range) const;
    bool ContainsInRanges(const TStringBuf& column) const;

    bool Overlaps(const TChannel& channel) const;
    bool Overlaps(const TRange& range) const;

    bool IsEmpty() const;

    NProto::TChannel ToProto() const;
    static TChannel FromProto(const NProto::TChannel& protoChannel);

    static TChannel FromYson(const NYTree::TYson& yson);
    static TChannel FromNode(NYTree::INodePtr node);

    const std::vector<Stroka>& GetColumns() const;

    //! Returns the channel containing all possible columns.
    static TChannel CreateUniversal();
    //! Returns the empty channel.
    static TChannel CreateEmpty();

private:
    TChannel();

    friend void operator -= (TChannel& lhs, const TChannel& rhs);

    std::vector<Stroka> Columns;
    std::vector<TRange> Ranges;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TChannel> ChannelsFromYson(const NYTree::TYson& yson);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
