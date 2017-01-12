#pragma once

#include <mapreduce/yt/interface/common.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IYsonConsumer;

void Serialize(const Stroka& value, IYsonConsumer* consumer);
void Serialize(const TStringBuf& value, IYsonConsumer* consumer);
void Serialize(const char* value, IYsonConsumer* consumer);
void Deserialize(Stroka& value, const TNode& node);

void Serialize(signed char value, IYsonConsumer* consumer);
void Serialize(short value, IYsonConsumer* consumer);
void Serialize(int value, IYsonConsumer* consumer);
void Serialize(long value, IYsonConsumer* consumer);
void Serialize(long long value, IYsonConsumer* consumer);
void Deserialize(i64& value, const TNode& node);

void Serialize(unsigned char value, IYsonConsumer* consumer);
void Serialize(unsigned short value, IYsonConsumer* consumer);
void Serialize(unsigned int value, IYsonConsumer* consumer);
void Serialize(unsigned long value, IYsonConsumer* consumer);
void Serialize(unsigned long long value, IYsonConsumer* consumer);
void Deserialize(ui64& value, const TNode& node);

void Serialize(double value, IYsonConsumer* consumer);
void Deserialize(ui64& value, const TNode& node);

void Serialize(bool value, IYsonConsumer* consumer);
void Deserialize(ui64& value, const TNode& node);

void Serialize(const TNode& node, IYsonConsumer* consumer);
void Deserialize(TNode& value, const TNode& node);

void Serialize(const TKey& key, IYsonConsumer* consumer);
void Serialize(const TKeyColumns& keyColumns, IYsonConsumer* consumer);

void Serialize(const TReadLimit& readLimit, IYsonConsumer* consumer);
void Serialize(const TReadRange& readRange, IYsonConsumer* consumer);
void Serialize(const TRichYPath& path, IYsonConsumer* consumer);
void Deserialize(TRichYPath& path, const TNode& node);

void Serialize(const TAttributeFilter& filter, IYsonConsumer* consumer);

void Serialize(const TColumnSchema& columnSchema, IYsonConsumer* consumer);
void Serialize(const TTableSchema& tableSchema, IYsonConsumer* consumer);
void Deserialize(TTableSchema& tableSchema, const TNode& node);
void Deserialize(TColumnSchema& columnSchema, const TNode& node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
