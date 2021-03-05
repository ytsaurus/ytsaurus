#include "yson_parser_adapter.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NClickHouseServer {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

bool TYsonParserAdapter::Object::find(const std::string_view & key, Element & result) const
{
    auto child = MapNode_->FindChild(TString(key));
    if (!child) {
        return false;
    }
    result = child;
    return true;
}

TYsonParserAdapter::Object::Iterator TYsonParserAdapter::Object::begin() const
{
    auto pairs = GetKeyValuePairs();
    return Iterator{pairs, 0};
}

TYsonParserAdapter::Object::Iterator TYsonParserAdapter::Object::end() const
{
    auto pairs = GetKeyValuePairs();
    return Iterator{pairs, pairs->size()};
}

std::shared_ptr<std::vector<std::pair<TString, NYTree::INodePtr>>> TYsonParserAdapter::Object::GetKeyValuePairs() const
{
    if (!KeyValuePairs_) {
        KeyValuePairs_ = std::make_shared<std::vector<std::pair<TString, NYTree::INodePtr>>>(MapNode_->GetChildren());
    }
    return KeyValuePairs_;
}

bool TYsonParserAdapter::parse(const std::string_view& yson, Element& result)
{
    try {
        Root_ = ConvertToNode(TYsonStringBuf(TStringBuf(yson.data(), yson.size())));
        result = Root_;
        return true;
    } catch (const std::exception& /* ex */) {
        return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
