#include "stdafx.h"
#include "yson_file_service.h"
#include "yson_reader.h"
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
        auto node = LoadFile();
        auto service = IYPathService::FromNode(~node);
        return TResolveResult::There(~service, path);
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
            auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
            builder->BeginTree();
            TYsonReader reader(~builder);
            TFileInput input(FileName);
            reader.Read(&input);
            return builder->EndTree();
        } catch (...) {
            throw yexception() << Sprintf("Error loading YSON file %s\n%s",
                ~FileName,
                ~CurrentExceptionMessage());
        }
    }
};

TYPathServiceProducer::TPtr CreateYsonFileProducer(const Stroka& fileName)
{
    return FromFunctor([=] () -> IYPathService::TPtr
        {
            return New<TYsonFileService>(fileName);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
