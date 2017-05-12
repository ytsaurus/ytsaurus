#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/schema.pb.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

// ToDo(psushin): deprecate.
//! Range of columns used as a part of channel description.
class TColumnRange
{
public:
    //! Creates a finite range.
    TColumnRange(const TString& begin, const TString& end);

    //! Creates an infinite range.
    explicit TColumnRange(const TString& begin = TString());

    TString Begin() const;
    TString End() const;

    bool Contains(const TStringBuf& value) const;
    bool Contains(const TColumnRange& range) const;
    bool Overlaps(const TColumnRange& range) const;

    bool IsInfinite() const;

private:
    bool IsInfinite_;
    TString Begin_;
    TString End_;
};

////////////////////////////////////////////////////////////////////////////////

//! Set of fixed columns and column ranges.
class TChannel
{
public:
    TChannel();
    TChannel(
        std::vector<TString> columns,
        std::vector<TColumnRange> ranges);

    void AddColumn(const TString& column);
    void AddRange(const TColumnRange& range);
    void AddRange(const TString& begin, const TString& end);

    bool Contains(const TStringBuf& column) const;
    bool Contains(const TChannel& channel) const;
    bool Contains(const TColumnRange& range) const;
    bool ContainsInRanges(const TStringBuf& column) const;

    bool Overlaps(const TChannel& channel) const;
    bool Overlaps(const TColumnRange& range) const;

    bool IsEmpty() const;
    bool IsUniversal() const;

    const std::vector<TString>& GetColumns() const;
    const std::vector<TColumnRange>& GetRanges() const;

    //! Returns the channel containing all possible columns.
    static const TChannel& Universal();

    //! Returns the empty channel.
    static const TChannel& Empty();

private:
    friend TChannel& operator -= (TChannel& lhs, const TChannel& rhs);

    std::vector<TString> Columns_;
    std::vector<TColumnRange> Ranges_;

};

bool operator==(const TChannel& lhs, const TChannel& rhs);
bool operator!=(const TChannel& lhs, const TChannel& rhs);

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TRange* protoRange, const TColumnRange& range);
void FromProto(TColumnRange* range, const NProto::TRange& protoRange);

void ToProto(NProto::TChannel* protoChannel, const TChannel& channel);
void FromProto(TChannel* channel, const NProto::TChannel& protoChannel);

void Serialize(const TChannel& channel, NYson::IYsonConsumer* consumer);
void Deserialize(TChannel& channel, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
