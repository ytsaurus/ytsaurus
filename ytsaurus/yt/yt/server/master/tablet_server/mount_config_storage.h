#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/attribute_set.h>

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/core/yson/pull_parser_deserialize.h>


namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TMountConfigStorage final
    : public NObjectServer::TAttributeSet
{
public:
    void Set(const TString& key, const NYson::TYsonString& value);
    bool Remove(const TString& key);

    void SetSelf(const NYson::TYsonString& value);
    void Clear();

    bool IsEmpty() const;

    NTabletNode::TCustomTableMountConfigPtr GetEffectiveConfig() const;

    //! Returns two nodes: the first one contains provided options recognized
    //! by TCustomTableMountConfig, the second one contains unrecognized options.
    std::pair<NYTree::IMapNodePtr, NYTree::IMapNodePtr> GetRecognizedConfig() const;

private:
    // Unwanted base class method intentionally shadowed.
    void TryInsert(const TString& key, const NYson::TYsonString& value);
};

DEFINE_REFCOUNTED_TYPE(TMountConfigStorage)

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TMountConfigStorage& storage, NYson::IYsonConsumer* consumer);
void Deserialize(TMountConfigStorage& storage, NYTree::INodePtr node);
void Deserialize(TMountConfigStorage& storage, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

namespace NYT::NYson::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <>
struct TIsPullParserDeserializable<NTabletServer::TMountConfigStoragePtr>
    : std::false_type
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson::NDetail
