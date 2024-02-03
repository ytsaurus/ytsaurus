#ifndef SERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include serialize.h"
// For the sake of sane code completion.
#include "serialize.h"
#endif

#include <yt/yt/server/node/tablet_node/object_detail.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

struct TRawObjectPtrSerializer
{
    template <class T>
    static void Save(NChaosNode::TSaveContext& context, T* object)
    {
        if (object) {
            auto key = object->GetDynamicData()->SerializationKey;
            YT_ASSERT(key != NullEntitySerializationKey);
            NYT::Save(context, key);
        } else {
            NYT::Save(context, NullEntitySerializationKey);
        }
    }

    template <class T>
    static void Load(NChaosNode::TLoadContext& context, T*& object)
    {
        using TObject = typename std::remove_pointer<T>::type;
        auto key = LoadSuspended<TEntitySerializationKey>(context);
        if (!key) {
            object = nullptr;
            SERIALIZATION_DUMP_WRITE(context, "objref <null>");
        } else {
            object = context.template GetRawEntity<TObject>(key);
            SERIALIZATION_DUMP_WRITE(context, "objref %v aka %v", object->GetId(), key);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename std::enable_if_t<
        std::is_convertible_v<T, const NTabletNode::TObjectBase*>
    >
>
{
    using TSerializer = NChaosNode::TRawObjectPtrSerializer;
    using TComparer = NTabletNode::TObjectIdComparer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
