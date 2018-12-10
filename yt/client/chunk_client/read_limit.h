#pragma once

#include "public.h"

#include <yt/client/chunk_client/proto/read_limit.pb.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/ytree/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TReadLimit
{
public:
    TReadLimit() = default;

    explicit TReadLimit(const NProto::TReadLimit& readLimit);
    explicit TReadLimit(NProto::TReadLimit&& readLimit);
    explicit TReadLimit(const std::unique_ptr<NProto::TReadLimit>& protoLimit);

    explicit TReadLimit(const NTableClient::TOwningKey& key);
    explicit TReadLimit(NTableClient::TOwningKey&& key);

    TReadLimit& operator= (const NProto::TReadLimit& protoLimit);
    TReadLimit& operator= (NProto::TReadLimit&& protoLimit);

    TReadLimit GetSuccessor() const;

    const NProto::TReadLimit& AsProto() const;

    const NTableClient::TOwningKey& GetKey() const;
    bool HasKey() const;
    TReadLimit& SetKey(const NTableClient::TOwningKey& key);
    TReadLimit& SetKey(NTableClient::TOwningKey&& key);

    i64 GetRowIndex() const;
    bool HasRowIndex() const;
    TReadLimit& SetRowIndex(i64 rowIndex);

    i64 GetOffset() const;
    bool HasOffset() const;
    TReadLimit& SetOffset(i64 offset);

    i64 GetChunkIndex() const;
    bool HasChunkIndex() const;
    TReadLimit& SetChunkIndex(i64 chunkIndex);

    bool IsTrivial() const;

    void MergeLowerKey(const NTableClient::TOwningKey& key);
    void MergeUpperKey(const NTableClient::TOwningKey& key);

    void MergeLowerRowIndex(i64 rowIndex);
    void MergeUpperRowIndex(i64 rowIndex);

    void Persist(const TStreamPersistenceContext& context);

    size_t SpaceUsed() const;

private:
    NProto::TReadLimit ReadLimit_;
    NTableClient::TOwningKey Key_;

    void InitKey();
    void InitCopy(const NProto::TReadLimit& readLimit);
    void InitMove(NProto::TReadLimit&& readLimit);

};

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TReadLimit& limit);

bool IsTrivial(const TReadLimit& limit);
bool IsTrivial(const NProto::TReadLimit& limit);

void ToProto(NProto::TReadLimit* protoReadLimit, const TReadLimit& readLimit);
void FromProto(TReadLimit* readLimit, const NProto::TReadLimit& protoReadLimit);

void Serialize(const TReadLimit& readLimit, NYson::IYsonConsumer* consumer);
void Deserialize(TReadLimit& readLimit, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

class TReadRange
{
public:
    TReadRange() = default;
    TReadRange(const TReadLimit& lowerLimit, const TReadLimit& upperLimit);
    explicit TReadRange(const TReadLimit& exact);

    explicit TReadRange(const NProto::TReadRange& range);
    explicit TReadRange(NProto::TReadRange&& range);
    TReadRange& operator= (const NProto::TReadRange& range);
    TReadRange& operator= (NProto::TReadRange&& range);

    DEFINE_BYREF_RW_PROPERTY(TReadLimit, LowerLimit);
    DEFINE_BYREF_RW_PROPERTY(TReadLimit, UpperLimit);

private:
    void InitCopy(const NProto::TReadRange& range);
    void InitMove(NProto::TReadRange&& range);
};

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TReadRange& range);

void ToProto(NProto::TReadRange* protoReadRange, const TReadRange& readRange);
void FromProto(TReadRange* readRange, const NProto::TReadRange& protoReadRange);

void Serialize(const TReadRange& readRange, NYson::IYsonConsumer* consumer);
void Deserialize(TReadRange& readRange, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
