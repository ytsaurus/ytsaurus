#include "poco_config.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NClickHouseServer {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TPocoConfigWrapper
    : public Poco::Util::AbstractConfiguration
{
private:
    IMapNodePtr Node_;

public:
    TPocoConfigWrapper(IMapNodePtr node)
        : Node_(std::move(node))
    { }

    bool getRaw(const std::string& key, std::string& value) const override
    {
        TNodeWalkOptions pocoConfigWalkOptions = FindNodeByYPathOptions;
        pocoConfigWalkOptions.NodeCannotHaveChildrenHandler = [] (const INodePtr& /*node*/) {
            return nullptr;
        };

        auto ypath = PocoPathToYPath(key);

        if (auto node = WalkNodeByYPath(Node_, ypath, pocoConfigWalkOptions)) {
            switch (node->GetType()) {
                case ENodeType::Int64:
                    value = std::to_string(node->AsInt64()->GetValue());
                    break;
                case ENodeType::Uint64:
                    value = std::to_string(node->AsUint64()->GetValue());
                    break;
                case ENodeType::Double:
                    value = std::to_string(node->AsDouble()->GetValue());
                    break;
                case ENodeType::Boolean:
                    value = node->AsBoolean()->GetValue() ? "true" : "false";
                    break;
                case ENodeType::String:
                    value = node->AsString()->GetValue();
                    break;
                default:
                    value = ConvertToYsonString(node, EYsonFormat::Text).AsStringBuf();
                    break;
            }
            return true;
        } else {
            return false;
        }
    }

    void setRaw(const std::string& key, const std::string& value) override
    {
        THROW_ERROR_EXCEPTION("Poco wrapper for INode cannot be modified")
            << TErrorAttribute("key", key)
            << TErrorAttribute("value", value);
    }


    void enumerate(const std::string& path, Keys& range) const override
    {
        auto ypath = PocoPathToYPath(path);

        if (auto node = FindNodeByYPath(Node_, ypath)) {
            if (node->GetType() == ENodeType::Map) {
                const auto& mapNode = node->AsMap();
                for (const auto& [key, value] : mapNode->GetChildren()) {
                    if (value->GetType() != ENodeType::List) {
                        range.emplace_back(key);
                    } else {
                        for (int index = 0; index < static_cast<int>(value->AsList()->GetChildCount()); ++index) {
                            range.emplace_back(Format("%v[%v]", key, index));
                        }
                    }
                }
            } else if (node->GetType() == ENodeType::List) {
                // TODO(max42): is this branch needed?
                const auto& listNode = node->AsList();
                for (int index = 0; index < static_cast<int>(listNode->GetChildCount()); ++index) {
                    range.emplace_back(Format("[%v]", index));
                }
            }
        } else {
            range = {};
        }
    }

private:
    static TYPath PocoPathToYPath(const std::string& key)
    {
        if (key.empty()) {
            return "";
        }
        TYPath result;
        // CH usues both ".abcd" and "abcd" forms for some reason.
        if (!key.starts_with(".")) {
            result = "/";
        }
        for (char c : key) {
            if (c == '.' || c == '[') {
                result.push_back('/');
            } else if (c != ']') {
                result.push_back(c);
            }
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

Poco::AutoPtr<Poco::Util::AbstractConfiguration> ConvertToPocoConfig(const INodePtr& node)
{
    if (node->GetType() != ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Only map nodes may be converted into Poco config, got node of type %v", node->GetType());
    }

    return new TPocoConfigWrapper(node->AsMap());
}

Poco::AutoPtr<Poco::Util::LayeredConfiguration> ConvertToLayeredConfig(const INodePtr& node)
{
    auto* layeredConfig = new Poco::Util::LayeredConfiguration();
    layeredConfig->add(ConvertToPocoConfig(node));
    return layeredConfig;
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
