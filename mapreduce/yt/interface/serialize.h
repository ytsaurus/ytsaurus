#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IYsonConsumer;

void Serialize(const TKey& key, IYsonConsumer* consumer);
void Serialize(const TKeyColumns& keyColumns, IYsonConsumer* consumer);

void Serialize(const TReadLimit& readLimit, IYsonConsumer* consumer);
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

void Serialize(const TGUID& path, IYsonConsumer* consumer);
void Deserialize(TGUID& value, const TNode& node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
