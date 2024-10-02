#include <yt/yt/orm/example/server/library/autogen/type_handler_impls.h>

namespace NYT::NOrm::NExample::NServer::NPlugins {

////////////////////////////////////////////////////////////////////////////////

class TCustomManualIdTypeHandler
    : public NLibrary::TManualIdTypeHandler
{
public:
    using NLibrary::TManualIdTypeHandler::TManualIdTypeHandler;

    void Initialize() override
    {
        TManualIdTypeHandler::Initialize();
        auto idEvaluator = [] (auto metaNode) -> std::optional<NYT::NOrm::NClient::NObjects::TObjectKey::TKeyField> {
            auto map = metaNode->AsMap();
            if (auto strIdNode = map->FindChild("str_id")) {
                return std::nullopt;
            }
            auto i64IdNode = map->FindChild("i64_id");
            auto ui64IdNode = map->FindChild("ui64_id");
            if (nullptr == i64IdNode || nullptr == ui64IdNode) {
                return std::nullopt;
            }
            return "abcde";
        };
        MetaAttributeSchema_->FindChild("str_id")->AsScalar()->SetKeyFieldEvaluator(std::move(idEvaluator));
    }
};

std::unique_ptr<NYT::NOrm::NServer::NObjects::IObjectTypeHandler> CreateManualIdTypeHandler(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TCustomManualIdTypeHandler>(bootstrap, config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NPlugins
