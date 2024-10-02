#include "custom_base_type_handler.h"

#include <yt/yt/orm/server/objects/attribute_schema.h>

namespace NYT::NOrm::NExample::NServer::NPlugins {

////////////////////////////////////////////////////////////////////////////////

void TObjectTypeHandlerBase::PostInitialize()
{
    TBase::PostInitialize();

    MetaAttributeSchema_->FindChild("ultimate_question_of_life")
        ->AsScalar()
        ->SetConstantChangedGetter(false)
        ->template SetValueGetter<NOrm::NServer::NObjects::TObject>(
            [] (
                NOrm::NServer::NObjects::TTransaction* /*transaction*/,
                const NOrm::NServer::NObjects::TObject* /*object*/,
                NYson::IYsonConsumer* consumer)
            {
                return NYTree::BuildYsonFluently(consumer).Value(42);
            })
        ->SetDefaultValueGetter(
            [] {
                return NYT::NYTree::BuildYsonNodeFluently().Value(0);
            })
        ->SetTimestampExpressionBuilder(
            [creationTimeSchema = MetaAttributeSchema_->FindChild("creation_time")] (
                NYT::NOrm::NServer::NObjects::IQueryContext* context,
                const NYPath::TYPath& /*path*/)
            {
                return creationTimeSchema->AsScalar()->RunTimestampExpressionBuilder(context, /*path*/{});
            });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NPlugins
