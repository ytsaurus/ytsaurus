#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/formats/parser.h>
#include <core/yson/consumer.h>

namespace NYT {
namespace NFormats {

// See json_writer.h for details on how YSON is mapped to JSON.
// This implementation of TJsonParser is DOM-based (and is thus suboptimal).

////////////////////////////////////////////////////////////////////////////////

class TJsonParser
    : public IParser
{
public:
    TJsonParser(
        NYson::IYsonConsumer* consumer,
        TJsonFormatConfigPtr config = NULL,
        NYson::EYsonType type = NYson::EYsonType::Node);

    virtual void Read(const TStringBuf& data);
    virtual void Finish();

    void Parse(TInputStream* input);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

void ParseJson(
    TInputStream* input,
    NYson::IYsonConsumer* consumer,
    TJsonFormatConfigPtr config = NULL,
    NYson::EYsonType type = NYson::EYsonType::Node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
