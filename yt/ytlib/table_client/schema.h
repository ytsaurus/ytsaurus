#pragma once

#include "common.h"
#include "value.h"
#include <ytlib/table_client/schema.pb.h>

#include <ytlib/ytree/public.h>
#include <ytlib/misc/property.h>

namespace NYT {
namespace NTableClient {

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

    static TChannel FromYson(const NYTree::TYson& yson);
    static TChannel FromNode(NYTree::INode* node);

    const std::vector<TColumn>& GetColumns() const;

    //! Returns the channel containing all possible columns.
    static TChannel CreateUniversal();
    //! Returns the empty channel.
    static TChannel CreateEmpty();

private:
    TChannel();

    friend void operator -= (TChannel& lhs, const TChannel& rhs);

    std::vector<TColumn> Columns;
    std::vector<TRange> Ranges;
};

////////////////////////////////////////////////////////////////////////////////

class TSchema
{
    DEFINE_BYREF_RW_PROPERTY(std::vector<TColumn>, KeyColumns);

public:
    static TSchema CreateDefault();

    void AddChannel(const TChannel& channel);

    const std::vector<TChannel>& GetChannels() const;

    static TSchema FromYson(const NYTree::TYson& yson);
    static TSchema FromNode(NYTree::INode* node);

private:
    class TConfig;

    TSchema();

    std::vector<TChannel> Channels;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
