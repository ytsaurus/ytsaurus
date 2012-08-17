#include "stdafx.h"
#include "yson_file_service.h"
#include "tree_builder.h"
#include "ephemeral.h"
#include "virtual.h"
#include "convert.h"

#include <ytlib/rpc/service.h>
#include <ytlib/rpc/server_detail.h>

namespace NYT {
namespace NYTree {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TWriteBackService
    : public IYPathService
{
public:
    typedef TIntrusivePtr<TWriteBackService> TPtr;

    TWriteBackService(
        const Stroka& fileName,
        INodePtr root,
        IYPathServicePtr underlyingService)
        : FileName(fileName)
        , Root(MoveRV(root))
        , UnderlyingService(underlyingService)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        auto result = UnderlyingService->Resolve(path, verb);
        if (result.IsHere()) {
            return TResolveResult::Here(result.GetPath());
        } else {
            return TResolveResult::There(
                New<TWriteBackService>(FileName, Root, result.GetService()),
                result.GetPath());
        }
    }

    virtual void Invoke(IServiceContextPtr context)
    {
        auto wrappedContext =
            UnderlyingService->IsWriteRequest(context)
            ? New<TReplyInterceptorContext>(
                context,
                BIND(&TWriteBackService::SaveFile, MakeStrong(this)))
            : context;
        UnderlyingService->Invoke(wrappedContext);
    }

    virtual Stroka GetLoggingCategory() const
    {
        return UnderlyingService->GetLoggingCategory();
    }

    virtual bool IsWriteRequest(IServiceContextPtr context) const
    {
        return UnderlyingService->IsWriteRequest(context);
    }

private:
    Stroka FileName;
    INodePtr Root;
    IYPathServicePtr UnderlyingService;

    void SaveFile()
    {
        try {
            TOFStream stream(FileName);
            // TODO(babenko): make format configurable
            WriteYson(&stream, ~Root, EYsonFormat::Pretty);
        } catch (const std::exception& ex) {
            throw yexception() << Sprintf("Error saving YSON file %s\n%s",
                ~FileName.Quote(),
                ex.what());
        }
    }
};

} // namespace

class TYsonFileService
    : public IYPathService
{
public:
    TYsonFileService(const Stroka& fileName)
        : FileName(fileName)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        UNUSED(verb);

        auto root = LoadFile();
        auto service = New<TWriteBackService>(FileName, ~root, ~root);
        return TResolveResult::There(service, path);
    }

    virtual void Invoke(NRpc::IServiceContextPtr context)
    {
        UNUSED(context);
        YUNREACHABLE();
    }

    virtual Stroka GetLoggingCategory() const
    {
        return "YsonFileService";
    }

    virtual bool IsWriteRequest(IServiceContextPtr context) const
    {
        UNUSED(context);
        YUNREACHABLE();
    }

private:
    Stroka FileName;

    INodePtr LoadFile()
    {
        try {
            TIFStream stream(FileName);
            return ConvertToNode(&stream);
        } catch (const std::exception& ex) {
            throw yexception() << Sprintf("Error loading YSON file %s\n%s",
                ~FileName.Quote(),
                ex.what());
        }
    }
};

TYPathServiceProducer CreateYsonFileProducer(const Stroka& fileName)
{
    return BIND([=] () -> IYPathServicePtr {
        return New<TYsonFileService>(fileName);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
