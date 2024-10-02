#include <yt/yt/orm/example/server/library/autogen/type_handler_impls.h>

namespace NYT::NOrm::NExample::NServer::NPlugins {

////////////////////////////////////////////////////////////////////////////////

class TEditorTypeHandler
    : public NLibrary::TEditorTypeHandler
{
    using NLibrary::TEditorTypeHandler::TEditorTypeHandler;

    void Initialize() override
    {
        NLibrary::TEditorTypeHandler::Initialize();

        SpecAttributeSchema_->FindChild("phone_number")
            ->AddUpdateHandler<NLibrary::TEditor>(std::bind_front(&TEditorTypeHandler::EditorPhoneNumberUpdate, this));
    }

    void EditorPhoneNumberUpdate(NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/, NLibrary::TEditor* editor)
    {
        static const TString invalidUtf8 = "\xc3\x28";
        auto& phone = editor->Spec().PhoneNumber().Load();
        if (phone == "change_me_through_store") {
            editor->Spec().PhoneNumber().Store(invalidUtf8);
        } else if (phone == "change_me_through_mutable_load") {
            *editor->Spec().PhoneNumber().MutableLoad() = invalidUtf8;
        }
    }
};

std::unique_ptr<NYT::NOrm::NServer::NObjects::IObjectTypeHandler> CreateEditorTypeHandler(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TEditorTypeHandler>(bootstrap, config);
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NOrm::NExample::NServer::NPlugins
