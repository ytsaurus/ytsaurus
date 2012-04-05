#pragma once

#include "yson_consumer.h"

#include <contrib/testing/framework.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TMockYsonConsumer
    : public IYsonConsumer
{
public:
    MOCK_METHOD2(OnStringScalar, void(const TStringBuf& value, bool hasAttributes));
    MOCK_METHOD2(OnIntegerScalar, void(i64 value, bool hasAttributes));
    MOCK_METHOD2(OnDoubleScalar, void(double value, bool hasAttributes));
    MOCK_METHOD1(OnEntity, void(bool hasAttributes));

    MOCK_METHOD0(OnBeginList, void());
    MOCK_METHOD0(OnListItem, void());
    MOCK_METHOD1(OnEndList, void(bool hasAttributes));

    MOCK_METHOD0(OnBeginMap, void());
    MOCK_METHOD1(OnMapItem, void(const TStringBuf& name));
    MOCK_METHOD1(OnEndMap, void(bool hasAttributes));

    MOCK_METHOD0(OnBeginAttributes, void());
    MOCK_METHOD1(OnAttributesItem, void(const TStringBuf& name));
    MOCK_METHOD0(OnEndAttributes, void());
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
