#pragma once

#include "public.h"

#include <ytlib/chunk_client/schema.pb.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <core/misc/phoenix.h>

#include <core/ytree/public.h>
#include <core/yson/consumer.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TReadLimit
{
public:
    TReadLimit();

    explicit TReadLimit(const NProto::TReadLimit& readLimit);
    explicit TReadLimit(NProto::TReadLimit&& readLimit);

    TReadLimit& operator= (const NProto::TReadLimit& protoLimit);
    TReadLimit& operator= (NProto::TReadLimit&& protoLimit);

    const NProto::TReadLimit& AsProto() const;

    const NVersionedTableClient::TOwningKey& GetKey() const;
    bool HasKey() const;
    void SetKey(const NVersionedTableClient::TOwningKey& key);
    void SetKey(NVersionedTableClient::TOwningKey&& key);

    i64 GetRowIndex() const;
    bool HasRowIndex() const;
    void SetRowIndex(i64 rowIndex);

    i64 GetOffset() const;
    bool HasOffset() const;
    void SetOffset(i64 offset);

    i64 GetChunkIndex() const;
    bool HasChunkIndex() const;
    void SetChunkIndex(i64 chunkIndex);

    bool IsTrivial() const;

    void Persist(NPhoenix::TPersistenceContext& context);

private:
    NProto::TReadLimit ReadLimit_;
    NVersionedTableClient::TOwningKey Key_;

    void InitKey();
    void InitCopy(const NProto::TReadLimit& readLimit);
    void InitMove(NProto::TReadLimit&& readLimit);

};

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TReadLimit& limit);

bool IsTrivial(const TReadLimit& limit);
bool IsTrivial(const NProto::TReadLimit& limit);

void ToProto(NProto::TReadLimit* protoReadLimit, const TReadLimit& readLimit);
void FromProto(TReadLimit* readLimit, const NProto::TReadLimit& protoReadLimit);

void Serialize(const TReadLimit& readLimit, NYson::IYsonConsumer* consumer);
void Deserialize(TReadLimit& readLimit, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
