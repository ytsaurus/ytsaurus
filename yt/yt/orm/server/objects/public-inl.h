#ifndef PUBLIC_INL_H_
#error "Direct inclusion of this file is not allowed, include public.h"
// For the sake of sane code completion.
#include "public.h"
#endif

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
struct TObjectPluginTraits
{
    using TType = TObject;

    static TObject* Upcast(TType* object)
    {
        return object;
    }

    static TType* Downcast(TObject* object)
    {
        return object;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
auto* DowncastObject(TObject* object)
{
    return TObjectPluginTraits<TObject>::Downcast(object);
}

template <class TObjectPlugin>
auto* UpcastObject(TObjectPlugin* object)
{
    return TObjectPluginTraits<TObjectPlugin>::Upcast(object);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
