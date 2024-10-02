#include <yt/yt/orm/example/server/library/autogen/type_handler_impls.h>

#include <util/datetime/base.h>

namespace NYT::NOrm::NExample::NServer::NPlugins {

////////////////////////////////////////////////////////////////////////////////

class TCatTypeHandler
    : public NLibrary::TCatTypeHandler
{
    using NLibrary::TCatTypeHandler::TCatTypeHandler;

    void Initialize() override
    {
        NLibrary::TCatTypeHandler::Initialize();

        SpecAttributeSchema_->FindChild("sleep_time")
            ->AsScalar()
            ->SetConstantChangedGetter(false)
            ->SetValueGetter<NLibrary::TCat>(
                std::bind_front(&TCatTypeHandler::Sleep, this))
            ->SetDefaultValueGetter(
            [] {
                return NYT::NYTree::BuildYsonNodeFluently().Value(false);
            });

        ControlAttributeSchema_->AddChildren({
            MakeScalarAttributeSchema("catch")
                ->SetMethod<NLibrary::TCat, NClient::NProto::NDataModel::TCatTestMethodRequest, NClient::NProto::NDataModel::TCatTestMethodResponse>(
                    [] (
                        NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/,
                        NLibrary::TCat* /*object*/,
                        const NYPath::TYPath& /*path*/,
                        const auto& value,
                        auto* response)
                    {
                        if (value.has_mouse() && !value.mouse().empty()) {
                            response->set_mood("nice");
                        } else {
                            response->set_mood("angry");
                        }
                    })
                ->SetValueSetter(
                    [] (
                        NYT::NOrm::NServer::NObjects::TTransaction*,
                        NYT::NOrm::NServer::NObjects::TObject*,
                        const NYPath::TYPath&,
                        const NYTree::INodePtr&,
                        bool,
                        std::optional<bool>,
                        NYT::NOrm::NClient::NNative::EAggregateMode,
                        const NYT::NOrm::NServer::NObjects::TTransactionCallContext&)
                    { }),
            MakeScalarAttributeSchema("pet")
                ->SetMethod<NLibrary::TCat, NClient::NProto::NDataModel::TVoid, NClient::NProto::NDataModel::TVoid>(
                    [] (
                        NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/,
                        NLibrary::TCat* /*object*/,
                        const NYPath::TYPath& /*path*/,
                        const auto& /*value*/,
                        auto* /*response*/)
                    { })
                ->SetValueSetter(
                    [] (
                        NYT::NOrm::NServer::NObjects::TTransaction*,
                        NYT::NOrm::NServer::NObjects::TObject*,
                        const NYPath::TYPath&,
                        const NYTree::INodePtr&,
                        bool,
                        std::optional<bool>,
                        NYT::NOrm::NClient::NNative::EAggregateMode,
                        const NYT::NOrm::NServer::NObjects::TTransactionCallContext&)
                    { }),
            });
    }

private:
    void Sleep(
        NOrm::NServer::NObjects::TTransaction* /*transaction*/,
        const NLibrary::TCat* /*cat*/,
        NYson::IYsonConsumer* consumer,
        const NYPath::TYPath& path)
    {
        NYPath::TTokenizer tokenizer(path);
        tokenizer.Skip(NYPath::ETokenType::StartOfStream);
        tokenizer.Skip(NYPath::ETokenType::Slash);
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto time = FromString<ui64>(tokenizer.GetLiteralValue());
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::EndOfStream);

        ::Sleep(TDuration::MilliSeconds(time));
        NYTree::BuildYsonFluently(consumer).Value(false);
    }
};

std::unique_ptr<NYT::NOrm::NServer::NObjects::IObjectTypeHandler> CreateCatTypeHandler(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TCatTypeHandler>(bootstrap, config);
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NOrm::NExample::NServer::NPlugins
