#include "stdafx.h"
#include "json_writer.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TJsonWriter::TJsonWriter(TOutputStream* stream, TJsonFormatConfigPtr config)
    : Stream(stream)
    , Config(config)
{
    if (!Config) {
        Config = New<TJsonFormatConfig>();
    }

    Writer.Reset(new NJson::TJsonWriter(Stream, Config->Pretty));
}

TJsonWriter::~TJsonWriter()
{
    Writer->Flush();
}

void TJsonWriter::OnStringScalar(const TStringBuf& value)
{
    Writer->Write(value);
}

void TJsonWriter::OnIntegerScalar(i64 value)
{
    Writer->Write(value);
}

void TJsonWriter::OnDoubleScalar(double value)
{
    Writer->Write(value);
}

void TJsonWriter::OnEntity()
{
    Writer->OpenMap();
    Writer->Write("$type", "entity");
    Writer->CloseMap();
}

void TJsonWriter::OnBeginList()
{
    Writer->OpenArray();
}

void TJsonWriter::OnListItem()
{ }

void TJsonWriter::OnEndList()
{
    Writer->CloseArray();
}

void TJsonWriter::OnBeginMap()
{
    Writer->OpenMap();
}

void TJsonWriter::OnKeyedItem(const TStringBuf& key)
{
    Writer->Write(key);
}

void TJsonWriter::OnEndMap()
{
    Writer->CloseMap();
}

void TJsonWriter::OnBeginAttributes()
{
    ythrow yexception() << "Attributes are not supported";
}

void TJsonWriter::OnEndAttributes()
{
    ythrow yexception() << "Attributes are not supported";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
