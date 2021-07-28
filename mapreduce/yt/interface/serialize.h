#pragma once

///
/// @file mapreduce/yt/interface/serialize.h
///
/// Header containing declaration of functions for serializing to/from YSON.

#include "common.h"

#include <library/cpp/type_info/fwd.h>

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

struct IYsonConsumer;

void Serialize(const TKey& key, IYsonConsumer* consumer);
void Deserialize(TKey& key, const TNode& node);

void Serialize(const TSortColumns& sortColumns, IYsonConsumer* consumer);
void Deserialize(TSortColumns& sortColumns, const TNode& node);

void Serialize(const TColumnNames& columnNames, IYsonConsumer* consumer);
void Deserialize(TColumnNames& columnNames, const TNode& node);

void Serialize(const TSortColumn& sortColumn, IYsonConsumer* consumer);
void Deserialize(TSortColumn& sortColumn, const TNode& node);

void Serialize(const TKeyBound& keyBound, IYsonConsumer* consumer);
void Deserialize(TKeyBound& keyBound, const TNode& node);

void Serialize(const TReadLimit& readLimit, IYsonConsumer* consumer);
void Deserialize(TReadLimit& readLimit, const TNode& node);

void Serialize(const TReadRange& readRange, IYsonConsumer* consumer);

void Serialize(const TRichYPath& path, IYsonConsumer* consumer);
void Deserialize(TRichYPath& path, const TNode& node);

void Serialize(const TAttributeFilter& filter, IYsonConsumer* consumer);

void Serialize(const TColumnSchema& columnSchema, IYsonConsumer* consumer);
void Serialize(const TTableSchema& tableSchema, IYsonConsumer* consumer);

void Deserialize(EValueType& valueType, const TNode& node);
void Deserialize(TTableSchema& tableSchema, const TNode& node);
void Deserialize(TColumnSchema& columnSchema, const TNode& node);
void Deserialize(TTableColumnarStatistics& statistics, const TNode& node);
void Deserialize(TTabletInfo& tabletInfos, const TNode& node);

void Serialize(const TGUID& path, IYsonConsumer* consumer);
void Deserialize(TGUID& value, const TNode& node);

void Serialize(const NTi::TTypePtr& type, IYsonConsumer* consumer);
void Deserialize(NTi::TTypePtr& type, const TNode& node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
