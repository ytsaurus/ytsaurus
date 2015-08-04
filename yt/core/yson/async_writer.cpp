#include "stdafx.h"
#include "async_writer.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

TAsyncYsonWriter::TAsyncYsonWriter()
    : SyncWriter_(&Stream_)
{ }

void TAsyncYsonWriter::OnStringScalar(const TStringBuf& value)
{
    SyncWriter_.OnStringScalar(value);
}

void TAsyncYsonWriter::OnInt64Scalar(i64 value)
{
    SyncWriter_.OnInt64Scalar(value);
}

void TAsyncYsonWriter::OnUint64Scalar(ui64 value)
{
    SyncWriter_.OnUint64Scalar(value);
}

void TAsyncYsonWriter::OnDoubleScalar(double value)
{
    SyncWriter_.OnDoubleScalar(value);
}

void TAsyncYsonWriter::OnBooleanScalar(bool value)
{
    SyncWriter_.OnBooleanScalar(value);
}

void TAsyncYsonWriter::OnEntity()
{
    SyncWriter_.OnEntity();
}

void TAsyncYsonWriter::OnBeginList()
{
    SyncWriter_.OnBeginList();
}

void TAsyncYsonWriter::OnListItem()
{
    SyncWriter_.OnListItem();
}

void TAsyncYsonWriter::OnEndList()
{
    SyncWriter_.OnEndList();
}

void TAsyncYsonWriter::OnBeginMap()
{
    SyncWriter_.OnBeginMap();
}

void TAsyncYsonWriter::OnKeyedItem(const TStringBuf& key)
{
    SyncWriter_.OnKeyedItem(key);
}

void TAsyncYsonWriter::OnEndMap()
{
    SyncWriter_.OnEndMap();
}

void TAsyncYsonWriter::OnBeginAttributes()
{
    SyncWriter_.OnBeginAttributes();
}

void TAsyncYsonWriter::OnEndAttributes()
{
    SyncWriter_.OnEndAttributes();
}

void TAsyncYsonWriter::OnRaw(const TStringBuf& yson, EYsonType type)
{
    SyncWriter_.OnRaw(yson, type);
}

void TAsyncYsonWriter::OnRaw(TFuture<TYsonString> asyncStr)
{
    FlushCurrentSegment();
    AsyncSegments_.emplace_back(std::move(asyncStr));
}

TFuture<TYsonString> TAsyncYsonWriter::Finish()
{
    FlushCurrentSegment();

    return Combine(AsyncSegments_).Apply(BIND([] (const std::vector<TYsonString>& segments) {
        size_t length = 0;
        for (const auto& segment : segments) {
            length += segment.Data().length();
        }

        Stroka result;
        result.reserve(length);
        for (const auto& segment : segments) {
            result.append(segment.Data());
        }

        return TYsonString(result);
    }));
}

void TAsyncYsonWriter::FlushCurrentSegment()
{
    if (!Stream_.Str().empty()) {
        AsyncSegments_.push_back(MakeFuture(TYsonString(Stream_.Str())));
        Stream_.Str().clear();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
