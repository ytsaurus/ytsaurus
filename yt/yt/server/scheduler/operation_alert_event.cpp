#include "operation_alert_event.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NScheduler {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

struct TSerializableOperationAlertEvent
    : public TOperationAlertEvent
    , public TYsonStructLite
{
    REGISTER_YSON_STRUCT_LITE(TSerializableOperationAlertEvent);

    static void Register(TRegistrar registrar)
    {
        registrar.BaseClassParameter("operation_id", &TOperationAlertEvent::OperationId)
            .Default()
            .DontSerializeDefault();;
        registrar.BaseClassParameter("alert_type", &TOperationAlertEvent::AlertType);
        registrar.BaseClassParameter("time", &TOperationAlertEvent::Time);
        registrar.BaseClassParameter("error", &TOperationAlertEvent::Error);
    }
};

void Serialize(const TOperationAlertEvent& operationAlertEvent, IYsonConsumer* consumer, bool serializeOperationId)
{
    auto wrapper = TSerializableOperationAlertEvent::Create();
    static_cast<TOperationAlertEvent&>(wrapper) = operationAlertEvent;
    if (!serializeOperationId) {
        static_cast<TOperationAlertEvent&>(wrapper).OperationId.reset();
    }
    Serialize(static_cast<const TYsonStructLite&>(wrapper), consumer);
}

void Deserialize(TOperationAlertEvent& operationAlertEvent, INodePtr node)
{
    auto wrapper = TSerializableOperationAlertEvent::Create();
    Deserialize(static_cast<TYsonStructLite&>(wrapper), node);
    operationAlertEvent = static_cast<TOperationAlertEvent&>(wrapper);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
