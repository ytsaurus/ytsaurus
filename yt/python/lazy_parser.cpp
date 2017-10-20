#include "lazy_parser.h"
#include "lazy_yson_consumer.h"

namespace NYT {
namespace NPython {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

Py::Object ParseLazyYson(
    IInputStream* inputStream,
    const TNullable<TString>& encoding,
    bool alwaysCreateAttributes,
    NYson::EYsonType ysonType)
{
    TPythonStringCache keyCacher(false, encoding);
    std::unique_ptr<NYson::TYsonParser> parser;
    auto stream = TStreamReader(inputStream);

    TLazyYsonConsumer consumer(BIND(
        [&] () {
            auto current = parser->GetCurrentPositionInBlock();
            if (!current) {
                current = stream.End();
            }
            return stream.ExtractPrefix(current);
        }),
        &keyCacher,
        encoding,
        alwaysCreateAttributes);

    parser.reset(new NYson::TYsonParser(&consumer, ysonType));

    if (ysonType == NYson::EYsonType::MapFragment) {
        consumer.OnBeginMap();
    }

    bool finished = false;
    while (!finished) {
        auto length = stream.End() - stream.Current();
        if (length == 0 && !stream.IsFinished()) {
            stream.RefreshBlock();
            continue;
        }

        if (length != 0) {
            parser->Read(stream.Current(), stream.End());
            stream.Advance(length);
        } else {
            finished = true;
        }
    }

    parser->Finish();
    if (ysonType == NYson::EYsonType::MapFragment) {
        consumer.OnEndMap();
    }

    return Py::Object(consumer.ExtractObject());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
