#pragma once

#include <mapreduce/yt/interface/common.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class IYsonConsumer;

void Serialize(const Stroka& value, IYsonConsumer* consumer);
void Serialize(const TStringBuf& value, IYsonConsumer* consumer);
void Serialize(const char* value, IYsonConsumer* consumer);

void Serialize(signed char value, IYsonConsumer* consumer);
void Serialize(unsigned char value, IYsonConsumer* consumer);
void Serialize(short value, IYsonConsumer* consumer);
void Serialize(unsigned short value, IYsonConsumer* consumer);
void Serialize(int value, IYsonConsumer* consumer);
void Serialize(unsigned int value, IYsonConsumer* consumer);
void Serialize(long value, IYsonConsumer* consumer);
void Serialize(unsigned long value, IYsonConsumer* consumer);
void Serialize(long long value, IYsonConsumer* consumer);
void Serialize(unsigned long long value, IYsonConsumer* consumer);

void Serialize(double value, IYsonConsumer* consumer);
void Serialize(bool value, IYsonConsumer* consumer);

void Serialize(const TNode& node, IYsonConsumer* consumer);

void Serialize(const TKey& key, IYsonConsumer* consumer);
void Serialize(const TKeyColumns& keyColumns, IYsonConsumer* consumer);
void Serialize(const TReadLimit& readLimit, IYsonConsumer* consumer);
void Serialize(const TReadRange& readRange, IYsonConsumer* consumer);
void Serialize(const TRichYPath& path, IYsonConsumer* consumer);
void Serialize(const TAttributeFilter& filter, IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
