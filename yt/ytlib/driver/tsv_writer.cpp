#include "stdafx.h"
#include "tsv_writer.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

TTsvWriter::TTsvWriter(TOutputStream* stream, TTsvWriterConfigPtr config)
    : Stream(stream)
    , FirstLine(true)
    , FirstItem(true)
{
    if (!config) {
        Config = New<TTsvWriterConfig>();
    }
}

void TTsvWriter::OnStringScalar(const TStringBuf& value)
{
    Stream->Write(value);
}

void TTsvWriter::OnIntegerScalar(i64 value)
{
    Stream->Write(ToString(value));
}

void TTsvWriter::OnDoubleScalar(double value)
{
    Stream->Write(ToString(value));
}

void TTsvWriter::OnEntity()
{
    ythrow yexception() << "Entities are not supported";
}

void TTsvWriter::OnBeginList()
{
    ythrow yexception() << "Lists are not supported";
}

void TTsvWriter::OnListItem()
{
    if (!FirstLine) {
        Stream->Write(Config->NewLineSeparator);
    }
    FirstItem = true;
    FirstLine = false;
}

void TTsvWriter::OnEndList()
{
    ythrow yexception() << "Lists are not supported";
}

void TTsvWriter::OnBeginMap()
{ }

void TTsvWriter::OnKeyedItem(const TStringBuf& key)
{
    if (!FirstItem) {
        Stream->Write(Config->ItemSeparator);
    }
    Stream->Write(key);
    Stream->Write(Config->KeyValueSeparator);
    FirstItem = false;
}

void TTsvWriter::OnEndMap()
{ }

void TTsvWriter::OnBeginAttributes()
{
    ythrow yexception() << "Attributes are not supported";
}

void TTsvWriter::OnEndAttributes()
{
    ythrow yexception() << "Attributes are not supported";
}

void TTsvWriter::OnRaw(const TStringBuf& yson, NYTree::EYsonType type)
{
    ythrow yexception() << "Raw data is not supported";
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NDriver
} // namespace NYT
