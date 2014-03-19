#pragma once

#include "cg_types.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TCGFragment
{
public:
    TCGFragment();
    ~TCGFragment();

    llvm::LLVMContext& GetContext();
    llvm::Module* GetModule();

    llvm::Function* GetRoutine(const Stroka& symbol);

    void Embody(llvm::Function* body);

    TCodegenedFunction GetCompiledBody();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

