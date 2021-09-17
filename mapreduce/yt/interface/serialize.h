#pragma once

///
/// @file mapreduce/yt/interface/serialize.h
///
/// Header containing declaration of functions for serializing to/from YSON.

#include "common.h"

#include <library/cpp/type_info/fwd.h>

namespace NYson {
struct IYsonConsumer;
} // namespace NYson

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Deserialize(TMaybe<T>& value, const TNode& node)
{
    value.ConstructInPlace();
    Deserialize(value.GetRef(), node);
}

template <class T>
void Deserialize(TVector<T>& value, const TNode& node)
{
    for (const auto& element : node.AsList()) {
        value.emplace_back();
        Deserialize(value.back(), element);
    }
}

template <class T>
void Deserialize(THashMap<TString, T>& value, const TNode& node)
{
    for (const auto& item : node.AsMap()) {
        Deserialize(value[item.first], item.second);
    }
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TKey& key, ::NYson::IYsonConsumer* consumer);
void Deserialize(TKey& key, const TNode& node);

void Serialize(const TSortColumns& sortColumns, ::NYson::IYsonConsumer* consumer);
void Deserialize(TSortColumns& sortColumns, const TNode& node);

void Serialize(const TColumnNames& columnNames, ::NYson::IYsonConsumer* consumer);
void Deserialize(TColumnNames& columnNames, const TNode& node);

void Serialize(const TSortColumn& sortColumn, ::NYson::IYsonConsumer* consumer);
void Deserialize(TSortColumn& sortColumn, const TNode& node);

void Serialize(const TKeyBound& keyBound, ::NYson::IYsonConsumer* consumer);
void Deserialize(TKeyBound& keyBound, const TNode& node);

void Serialize(const TReadLimit& readLimit, ::NYson::IYsonConsumer* consumer);
void Deserialize(TReadLimit& readLimit, const TNode& node);

void Serialize(const TReadRange& readRange, ::NYson::IYsonConsumer* consumer);

void Serialize(const TRichYPath& path, ::NYson::IYsonConsumer* consumer);
void Deserialize(TRichYPath& path, const TNode& node);

void Serialize(const TAttributeFilter& filter, ::NYson::IYsonConsumer* consumer);

void Serialize(const TColumnSchema& columnSchema, ::NYson::IYsonConsumer* consumer);
void Serialize(const TTableSchema& tableSchema, ::NYson::IYsonConsumer* consumer);

void Deserialize(EValueType& valueType, const TNode& node);
void Deserialize(TTableSchema& tableSchema, const TNode& node);
void Deserialize(TColumnSchema& columnSchema, const TNode& node);
void Deserialize(TTableColumnarStatistics& statistics, const TNode& node);
void Deserialize(TTabletInfo& tabletInfos, const TNode& node);

void Serialize(const TGUID& path, ::NYson::IYsonConsumer* consumer);
void Deserialize(TGUID& value, const TNode& node);

void Serialize(const NTi::TTypePtr& type, ::NYson::IYsonConsumer* consumer);
void Deserialize(NTi::TTypePtr& type, const TNode& node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
