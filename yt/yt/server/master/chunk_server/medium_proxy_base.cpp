#include "medium_proxy_base.h"

#include "chunk_manager.h"
#include "medium_base.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

void TMediumProxyBase::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    TBase::ListSystemAttributes(descriptors);

    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Name)
        .SetWritable(true)
        .SetReplicated(true)
        .SetMandatory(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Index)
        .SetReplicated(true)
        .SetMandatory(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Priority)
        .SetWritable(true)
        .SetReplicated(true));
    descriptors->push_back(EInternedAttributeKey::Domestic);
}

bool TMediumProxyBase::GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer)
{
    const auto* medium = GetThisImpl();

    switch (key) {
        case EInternedAttributeKey::Name:
            BuildYsonFluently(consumer)
                .Value(medium->GetName());
            return true;

        case EInternedAttributeKey::Index:
            BuildYsonFluently(consumer)
                .Value(medium->GetIndex());
            return true;

        case EInternedAttributeKey::Priority:
            BuildYsonFluently(consumer)
                .Value(medium->GetPriority());
            return true;

        case EInternedAttributeKey::Domestic:
            BuildYsonFluently(consumer)
                .Value(medium->IsDomestic());
            return true;

        default:
            break;
    }

    return TBase::GetBuiltinAttribute(key, consumer);
}

bool TMediumProxyBase::SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force)
{
    auto* medium = GetThisImpl();
    const auto& chunkManager = Bootstrap_->GetChunkManager();

    switch (key) {
        case EInternedAttributeKey::Name: {
            auto newName = ConvertTo<TString>(value);
            chunkManager->RenameMedium(medium, newName);
            return true;
        }

        case EInternedAttributeKey::Priority: {
            auto newPriority = ConvertTo<int>(value);
            chunkManager->SetMediumPriority(medium, newPriority);
            return true;
        }

        default:
            break;
    }

    return TBase::SetBuiltinAttribute(key, value, force);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
