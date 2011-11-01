#include "stdafx.h"
#include "virtual.h"
#include "fluent.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

IYPathService::TNavigateResult TVirtualMapBase::Navigate(TYPath path)
{
    UNUSED(path);
    ythrow TYTreeException() << "Navigation is not supported";
}

IYPathService::TSetResult TVirtualMapBase::Set(TYPath path, TYsonProducer::TPtr producer)
{
    UNUSED(path);
    ythrow TYTreeException() << "Node is read-only";
}

IYPathService::TRemoveResult TVirtualMapBase::Remove(TYPath path)
{
    UNUSED(path);
    ythrow TYTreeException() << "Node is read-only";
}

IYPathService::TLockResult TVirtualMapBase::Lock(TYPath path)
{
    UNUSED(path);
    ythrow TYTreeException() << "Locking is not supported";
}

IYPathService::TGetResult TVirtualMapBase::Get(TYPath path, IYsonConsumer* consumer)
{
    // TODO: attributes?

    if (path.Empty()) {
        auto keys = GetKeys();
        // TODO: refactor using fluent API
        consumer->OnBeginMap();
        FOREACH (const auto& key, keys) {
            consumer->OnMapItem(key);
            auto service = GetItemService(key);
            YASSERT(~service != NULL);
            // TODO: use constant for /
            GetYPath(service, "/", consumer);
        }
        consumer->OnEndMap(false);
    } else {
        Stroka prefix;
        TYPath tailPath;
        ChopYPathPrefix(path, &prefix, &tailPath);

        auto service = GetItemService(prefix);
        if (~service == NULL) {
            ythrow TYTreeException() << Sprintf("Key %s is not found",
                ~prefix.Quote());
        }

        return TGetResult::CreateRecurse(service, tailPath);
    }
    return TGetResult::CreateDone();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

