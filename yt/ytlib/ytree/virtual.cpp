#include "stdafx.h"
#include "virtual.h"
#include "fluent.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TVirtualMapBase::Resolve(TYPath path, bool mustExist)
{
    UNUSED(path);
    UNUSED(mustExist);
    ythrow yexception() << "Further navigation is not supported";
}

void TVirtualMapBase::Invoke(NRpc::IServiceContext* context)
{
    UNUSED(context);
}

//IYPathService::TGetResult TVirtualMapBase::Get(TYPath path, IYsonConsumer* consumer)
//{
//    // TODO: attributes?
//
//    if (path.Empty()) {
//        auto keys = GetKeys();
//        // TODO: refactor using fluent API
//        consumer->OnBeginMap();
//        FOREACH (const auto& key, keys) {
//            consumer->OnMapItem(key);
//            auto service = GetItemService(key);
//            YASSERT(~service != NULL);
//            // TODO: use constant for /
//            GetYPath(service, "/", consumer);
//        }
//        consumer->OnEndMap(false);
//    } else {
//        Stroka prefix;
//        TYPath suffixPath;
//        ChopYPathPrefix(path, &prefix, &suffixPath);
//
//        auto service = GetItemService(prefix);
//        if (~service == NULL) {
//            ythrow TYTreeException() << Sprintf("Key %s is not found",
//                ~prefix.Quote());
//        }
//
//        return TGetResult::CreateRecurse(service, suffixPath);
//    }
//    return TGetResult::CreateDone();
//}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

