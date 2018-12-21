#include "poco_config.h"

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/node.h>
#include <yt/core/ytree/ypath_client.h>

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
        auto ypath = PocoPathToYPath(key);

        if (auto node = FindNodeByYPath(Node_, ypath)) {
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
                    value = ConvertToYsonString(node, EYsonFormat::Text).GetData();
                    break;
            }
            return true;
        } else {
            return false;
        }
    }

    virtual void setRaw(const std::string& key, const std::string& value)
    {
        THROW_ERROR_EXCEPTION("Poco wrapper for INode cannot be modified")
            << TErrorAttribute("key", key)
            << TErrorAttribute("value", value);
    }


    void enumerate(const std::string& key, Keys& range) const override
    {
        auto ypath = PocoPathToYPath(key);

        if (auto node = FindNodeByYPath(Node_, ypath)) {
            const auto& mapNode = node->AsMap();
            auto keys = mapNode->GetKeys();
            range = std::vector<std::string>(keys.begin(), keys.end());
        } else {
            range = {};
        }
    }

private:
    static TYPath PocoPathToYPath(const std::string& key) {
        auto ypath = TString("/" + key);
        for (char& c : ypath) {
            if (c == '.') {
                c = '/';
            }
        }
        return ypath;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
