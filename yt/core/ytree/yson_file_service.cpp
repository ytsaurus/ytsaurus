#include "stdafx.h"
#include "yson_file_service.h"
#include "tree_builder.h"
#include "ephemeral_node_factory.h"
#include "virtual.h"
#include "convert.h"

#include <core/ytree/exception_helpers.h>

#include <core/rpc/service_detail.h>
#include <core/rpc/server_detail.h>

namespace NYT {
namespace NYTree {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TYsonFileService
    : public TYPathServiceBase
{
public:
    explicit TYsonFileService(const Stroka& fileName)
        : FileName(fileName)
    { }

    virtual TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr context) override
    {
        const auto& method = context->GetMethod();
        if (method != "Get" && method != "List") {
            ThrowMethodNotSupported(method);
        }

        auto root = LoadFile();
        return TResolveResult::There(root, path);
    }

private:
    Stroka FileName;

    INodePtr LoadFile()
    {
        try {
            TIFStream stream(FileName);
            return ConvertToNode(&stream);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error loading YSON file %Qv",
                FileName)
                << ex;
        }
    }
};

IYPathServicePtr CreateYsonFileService(const Stroka& fileName)
{
    return New<TYsonFileService>(fileName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
