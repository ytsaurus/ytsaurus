#include "string.h"
#include "stream.h"
#include "null_consumer.h"
#include "parser.h"
#include "consumer.h"
#include "pull_parser.h"

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

void ValidateYson(const TYsonStringBuf& str, int nestingLevelLimit)
{
    if (str) {
        TMemoryInput input(str.AsStringBuf());
        TYsonPullParser parser(&input, str.GetType(), nestingLevelLimit);
        auto cursor = TYsonPullParserCursor(&parser);
        cursor.SkipComplexValue();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TBinaryYsonStringSerializer::Save(TStreamSaveContext& context, const TYsonString& str)
{
    using NYT::Save;
    if (str) {
        Save(context, static_cast<i32>(str.GetType()));
        auto strBuf = str.AsStringBuf();
        TSizeSerializer::Save(context, strBuf.length());
        TRangeSerializer::Save(context, TRef::FromStringBuf(strBuf));
    } else {
        Save(context, static_cast<i32>(-1));
    }
}

void TBinaryYsonStringSerializer::Load(TStreamLoadContext& context, TYsonString& str)
{
    using NYT::Load;
    auto type = Load<i32>(context);
    if (type != -1) {
        struct TLoadedYsonStringPayload
            : public TRefCounted
            , public TWithExtraSpace<TLoadedYsonStringPayload>
        {
            char* GetData()
            {
                return static_cast<char*>(GetExtraSpacePtr());
            }
        };

        auto size = TSizeSerializer::Load(context);
        auto payload = NewWithExtraSpace<TLoadedYsonStringPayload>(size);
        TRangeSerializer::Load(context, TMutableRef(payload->GetData(), size));
        auto ref = TRef(payload->GetData(), size);
        auto sharedRef = TSharedRef(ref, std::move(payload));
        str = TYsonString(std::move(sharedRef), static_cast<EYsonType>(type));
    } else {
        str = TYsonString();
    }
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonString& yson, IYsonConsumer* consumer)
{
    consumer->OnRaw(yson);
}

void Serialize(const TYsonStringBuf& yson, IYsonConsumer* consumer)
{
    consumer->OnRaw(yson);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
