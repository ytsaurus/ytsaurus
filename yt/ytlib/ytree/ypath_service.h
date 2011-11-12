#pragma once

#include "common.h"
#include "yson_events.h"

#include "../misc/property.h"
#include "../rpc/service.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

DECLARE_POLY_ENUM2(EYPathErrorCode, NRpc::EErrorCode,
    ((GenericError)(100))
);

////////////////////////////////////////////////////////////////////////////////

struct IYPathService
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<IYPathService> TPtr;

    class TNavigateResult
    {
        DECLARE_BYVAL_RO_PROPERTY(Service, IYPathService::TPtr);
        DECLARE_BYVAL_RO_PROPERTY(Path, TYPath);

    public:
        static TNavigateResult Here(TYPath path)
        {
            TNavigateResult result;
            result.Path_ = path;
            return result;
        }

        static TNavigateResult There(IYPathService* service, TYPath path)
        {
            TNavigateResult result;
            result.Service_ = service;
            result.Path_ = path;
            return result;
        }

        bool IsHere() const
        {
            return ~Service_ == NULL;
        }
    };

    virtual TNavigateResult Navigate(TYPath path, bool mustExist) = 0;

    virtual void Invoke(NRpc::IServiceContext* context) = 0;

    static IYPathService::TPtr FromNode(INode* node);
    static IYPathService::TPtr FromProducer(TYsonProducer* producer);
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NYTree
} // namespace NYT
