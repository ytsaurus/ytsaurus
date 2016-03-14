#ifndef DEFAULT_BUILDING_CONSUMER_INL_H_
#error "Direct inclusion of this file is not allowed, include default_building_consumer.h"
#endif

#include "ephemeral_node_factory.h"
#include "tree_builder.h"

#include <yt/core/yson/forwarding_consumer.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
class TBuildingYsonConsumerViaTreeBuilder
    : public NYson::TForwardingYsonConsumer
    , public NYson::IBuildingYsonConsumer<T>
{
public:
    TBuildingYsonConsumerViaTreeBuilder(NYson::EYsonType ysonType)
        : TreeBuilder_(CreateBuilderFromFactory(CreateEphemeralNodeFactory()))
        , YsonType_(ysonType)
    { 
        TreeBuilder_->BeginTree();

        switch (YsonType_) {
            case NYson::EYsonType::ListFragment:
                TreeBuilder_->OnBeginList();
                break;
            case NYson::EYsonType::MapFragment:
                TreeBuilder_->OnBeginMap();
                break;
            default:
                break;
        }

        Forward(TreeBuilder_.get());
    }

    virtual T Finish()
    {
        switch (YsonType_) {
            case NYson::EYsonType::ListFragment:
                TreeBuilder_->OnEndList();
                break;
            case NYson::EYsonType::MapFragment:
                TreeBuilder_->OnEndMap();
                break;
            default:
                break;
        }

        T result;
        Deserialize(result, TreeBuilder_->EndTree());
        return result;
    } 

private:
    std::unique_ptr<ITreeBuilder> TreeBuilder_;
    NYson::EYsonType YsonType_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <class T>
void CreateBuildingYsonConsumer(std::unique_ptr<NYson::IBuildingYsonConsumer<T>>* buildingConsumer, NYson::EYsonType ysonType)
{
    *buildingConsumer = std::make_unique<TBuildingYsonConsumerViaTreeBuilder<T>>(ysonType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
