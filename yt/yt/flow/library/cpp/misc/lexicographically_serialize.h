#pragma once

#include <yt/yt/client/table_client/public.h>

#include <util/generic/fwd.h>
#include <util/generic/strbuf.h>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Library for translating composite data structures to human-readable ascii-strings with the same order of comparison.

//! For every x:
//! LexicographicallySerialize(x) == LexicographicallySerialize(x);
//! LexicographicallyParse<T>(LexicographicallySerialize(x)) == x;
//! LexicographicallySerialize(x) does not contain chars less than ' ' and greater than 127.
//! For every x != y of one type:
//! LexicographicallySerialize(x) is not a prefix of LexicographicallySerialize(y).
//! For every x < y of one type:
//! LexicographicallySerialize(x) < LexicographicallySerialize(y).

std::string LexicographicallySerialize(ui64 value);
std::string LexicographicallySerialize(i64 value);
std::string LexicographicallySerialize(TStringBuf value);
std::string LexicographicallySerialize(const std::vector<std::string>& value);

//! Serialized unversioned row with simple types: Null, Int64, Uint64, String.
//! Serialized values are comparable iff they have equal column types on common prefix.
//! (5, "string") is not comparable with ("string", 5) - result in undefined.
//! ("string") is comparable with ("string", 5).
std::string LexicographicallySerializeUnversionedRowV1(const NTableClient::TUnversionedRow& value);

//! Serialized unversioned row with simple types: Min, TheBottom, Null, Int64, Uint64, String, Max.
//! Notice, that set is different from LexicographicallySerializeUnversionedRowV1.
//! Serialized values are always comparable, even it they have different column types and length.
std::string LexicographicallySerializeUnversionedRowV2(const NTableClient::TUnversionedRow& value);

//! Read part, write parsed value to |destination| and skip read bytes in |serialized|.
void LexicographicallyRead(TStringBuf& serialized, ui64& destination);
void LexicographicallyRead(TStringBuf& serialized, i64& destination);
void LexicographicallyRead(TStringBuf& serialized, std::string& destination);
void LexicographicallyRead(TStringBuf& serialized, std::vector<std::string>& destination);

//! Parse whole |serialized| as T.
template <typename T>
T LexicographicallyParse(TStringBuf serialized);

//! Make string with reverse order. |serialized| must be serialized value with fulfilled invariants.
std::string RevertLexicographicallySerialized(TStringBuf serialized);
//! Revert reversing.
std::string UndoRevertLexicographicallySerialized(TStringBuf revertedSerialized);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define LEXICOGRAPHICALLY_SERIALIZE_INL_H_
#include "lexicographically_serialize-inl.h"
#undef LEXICOGRAPHICALLY_SERIALIZE_INL_H_
