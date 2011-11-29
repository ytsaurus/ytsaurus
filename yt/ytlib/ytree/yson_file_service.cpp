#include "stdafx.h"
#include "yson_file_service.h"
#include "serialize.h"
#include "tree_builder.h"
#include "ephemeral.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

// TOOD: read-only?
class TYsonFileService
    : public IYPathService
{
public:
    TYsonFileService(const Stroka& fileName)
        : FileName(fileName)
    { }

    virtual TResolveResult Resolve(TYPath path, const Stroka& verb)
    {
        UNUSED(verb);

        auto node = LoadFile();
        return TResolveResult::There(~IYPathService::FromNode(~node), path);
    }

    virtual void Invoke(NRpc::IServiceContext* context)
    {
        UNUSED(context);
        ythrow yexception() << "Resolution error: direct invocation is forbidden";
    }

private:
    Stroka FileName;

    INode::TPtr LoadFile()
    {
        try {
            return DeserializeFromYson(FileName);
        } catch (...) {
            throw yexception() << Sprintf("Error loading YSON file %s\n%s",
                ~FileName,
                ~CurrentExceptionMessage());
        }
    }
};

TYPathServiceProvider::TPtr CreateYsonFileProvider(const Stroka& fileName)
{
    return FromFunctor([=] () -> IYPathService::TPtr
        {
            return New<TYsonFileService>(fileName);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
