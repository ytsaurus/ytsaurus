#include "stdafx.h"
#include "json_writer.h"
#include "config.h"

#include <ytlib/ytree/null_yson_consumer.h>
#include <ytlib/misc/assert.h>

#include <util/string/base64.h>

// XXX(sandello): This is a direct hack to yajl's core just to not to implement
// in-house UTF8 validator.

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {
bool IsValidUtf8(const unsigned char* buffer, size_t length)
{
    YASSERT(buffer);
    YASSERT(length);

    const unsigned char* s = buffer;
    while (length--) {
        if (*s <= 0x7F) {
        } else if ((*s >> 5) == 0x06) {
            ++s;
            if (!((*s >> 6) == 0x2)) return false;
        } else if ((*s >> 4) == 0x0E) {
            ++s;
            if (!((*s >> 6) == 0x2)) return false;
            ++s;
            if (!((*s >> 6) == 0x2)) return false;
        } else if ((*s >> 3) == 0x1E) {
            ++s;
            if (!((*s >> 6) == 0x2)) return false;
            ++s;
            if (!((*s >> 6) == 0x2)) return false;
            ++s;
            if (!((*s >> 6) == 0x2)) return false;
        } else {
            return false;
        }
        ++s;
    }

    return true;
}
} // namespace anonymous

////////////////////////////////////////////////////////////////////////////////

TJsonWriter::TJsonWriter(TOutputStream* output, TJsonFormatConfigPtr config)
    : Config(config)
{
    if (!Config) {
        Config = New<TJsonFormatConfig>();
    }
    UnderlyingJsonWriter.Reset(new NJson::TJsonWriter(output, Config->Pretty));
    JsonWriter = ~UnderlyingJsonWriter;
    HasAttributes_ = false;
    InAttributesBalance_ = 0;
}

TJsonWriter::~TJsonWriter()
{
    Flush();
}

void TJsonWriter::EnterNode()
{
    if (Config->PrintAttributes == EPrintAttributes::Never) {
        HasAttributes_ = false;
    }
    else if (Config->PrintAttributes == EPrintAttributes::OnDemand) {
        // Do nothing
    }
    else if (Config->PrintAttributes == EPrintAttributes::Always) {
        if (!HasAttributes_) {
            JsonWriter->OpenMap();
            JsonWriter->Write("$attributes");
            JsonWriter->OpenMap();
            JsonWriter->CloseMap();
        }
        HasAttributes_ = true;
    }
    HasUnfoldedStructureStack_.push_back(HasAttributes_);

    if (HasAttributes_) {
        JsonWriter->Write("$value");
        HasAttributes_ = false;
    }
}

void TJsonWriter::LeaveNode()
{
    YCHECK(!HasUnfoldedStructureStack_.empty());
    if (HasUnfoldedStructureStack_.back()) {
        // Close map of the {$attributes, $value}
        JsonWriter->CloseMap();
    }
    HasUnfoldedStructureStack_.pop_back();
}

bool TJsonWriter::IsWriteAllowed()
{
    if (Config->PrintAttributes == EPrintAttributes::Never) {
        return InAttributesBalance_ == 0;
    }
    return true;
}


void TJsonWriter::OnStringScalar(const TStringBuf& value)
{
    if (IsWriteAllowed()) {
        EnterNode();
        WriteStringScalar(value);
        LeaveNode();
    }
}

void TJsonWriter::OnIntegerScalar(i64 value)
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter->Write(value);
        LeaveNode();
    }
}

void TJsonWriter::OnDoubleScalar(double value)
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter->Write(value);
        LeaveNode();
    }
}

void TJsonWriter::OnEntity()
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter->WriteNull();
        LeaveNode();
    }
}

void TJsonWriter::OnBeginList()
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter->OpenArray();
    }
}

void TJsonWriter::OnListItem()
{ }

void TJsonWriter::OnEndList()
{
    if (IsWriteAllowed()) {
        JsonWriter->CloseArray();
        LeaveNode();
    }
}

void TJsonWriter::OnBeginMap()
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter->OpenMap();
    }
}

void TJsonWriter::OnKeyedItem(const TStringBuf& name)
{
    if (IsWriteAllowed()) {
        WriteStringScalar(name);
    }
}

void TJsonWriter::OnEndMap()
{
    if (IsWriteAllowed()) {
        JsonWriter->CloseMap();
        LeaveNode();
    }
}

void TJsonWriter::OnBeginAttributes()
{
    InAttributesBalance_ += 1;
    if (Config->PrintAttributes != EPrintAttributes::Never) {
        JsonWriter->OpenMap();
        JsonWriter->Write("$attributes");
        JsonWriter->OpenMap();
    }
}

void TJsonWriter::OnEndAttributes()
{
    InAttributesBalance_ -= 1;
    if (Config->PrintAttributes != EPrintAttributes::Never) {
        HasAttributes_ = true;
        JsonWriter->CloseMap();
    }
}

TJsonWriter::TJsonWriter(NJson::TJsonWriter* jsonWriter, TJsonFormatConfigPtr config)
    : JsonWriter(jsonWriter)
    , Config(config)
{ }

void TJsonWriter::WriteStringScalar(const TStringBuf &value)
{
    if (
        value.empty() ||
        (value[0] != '&' && IsValidUtf8(
            reinterpret_cast<const unsigned char*>(value.c_str()),
            value.length()))
    ) {
        JsonWriter->Write(value);
    } else {
        JsonWriter->Write("&" + Base64Encode(value));
    }
}

void TJsonWriter::Flush()
{
    JsonWriter->Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
