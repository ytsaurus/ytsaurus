#pragma once

#include "public.h"

#include <ytlib/chunk_client/schema.pb.h>

#include <core/ytree/public.h>
#include <core/yson/consumer.h>

namespace NYT {
namespace NChunkClient {

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

    // TODO(sandello): Migrate to core/misc/protobuf_helpers.h.
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
    TChannel();

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
    bool IsUniversal() const;

    // TODO(sandello): Migrate to core/misc/protobuf_helpers.h.
    NProto::TChannel ToProto() const;
    static TChannel FromProto(const NProto::TChannel& protoChannel);

    const std::vector<Stroka>& GetColumns() const;
    const std::vector<TRange>& GetRanges() const;

    //! Returns the channel containing all possible columns.
    static const TChannel& Universal();

    //! Returns the empty channel.
    static const TChannel& Empty();

private:
    friend TChannel& operator -= (TChannel& lhs, const TChannel& rhs);

    std::vector<Stroka> Columns_;
    std::vector<TRange> Ranges_;

};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TChannel& channel, NYson::IYsonConsumer* consumer);
void Deserialize(TChannel& channel, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void Serialize(const TReadLimit& readLimit, NYson::IYsonConsumer* consumer);
void Deserialize(TReadLimit& readLimit, NYTree::INodePtr node);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

bool IsNontrivial(const NProto::TReadLimit& limit);
bool IsTrivial(const NProto::TReadLimit& limit);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
