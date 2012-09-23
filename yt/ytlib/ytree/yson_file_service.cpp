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
    TWriteBackService(
        const Stroka& fileName,
        INodePtr root,
        IYPathServicePtr underlyingService)
        : FileName(fileName)
        , Root(MoveRV(root))
        , UnderlyingService(underlyingService)
    { }

    virtual TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr context) override
    {
        auto result = UnderlyingService->Resolve(path, context);
        if (result.IsHere()) {
            return TResolveResult::Here(result.GetPath());
        } else {
            return TResolveResult::There(
                New<TWriteBackService>(FileName, Root, result.GetService()),
                result.GetPath());
        }
    }

    virtual void Invoke(IServiceContextPtr context) override
    {
        auto wrappedContext =
            UnderlyingService->IsWriteRequest(context)
            ? New<TReplyInterceptorContext>(
                context,
                BIND(&TWriteBackService::SaveFile, MakeStrong(this)))
            : context;
        UnderlyingService->Invoke(wrappedContext);
    }

    virtual Stroka GetLoggingCategory() const override
    {
        return UnderlyingService->GetLoggingCategory();
    }

    virtual bool IsWriteRequest(IServiceContextPtr context) const override
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
            THROW_ERROR_EXCEPTION("Error saving YSON file %s", ~FileName.Quote())
                << ex;
        }
    }
};

} // namespace

class TYsonFileService
    : public IYPathService
{
public:
    explicit TYsonFileService(const Stroka& fileName)
        : FileName(fileName)
    { }

    virtual TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr context) override
    {
        UNUSED(context);

        auto root = LoadFile();
        auto service = New<TWriteBackService>(FileName, ~root, ~root);
        return TResolveResult::There(service, path);
    }

    virtual void Invoke(NRpc::IServiceContextPtr context) override
    {
        UNUSED(context);
        YUNREACHABLE();
    }

    virtual Stroka GetLoggingCategory() const override
    {
        return "YsonFileService";
    }

    virtual bool IsWriteRequest(IServiceContextPtr context) const override
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
            THROW_ERROR_EXCEPTION("Error loading YSON file %s", ~FileName.Quote())
                << ex;
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
