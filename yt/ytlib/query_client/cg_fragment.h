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

    void SetMainFunction(llvm::Function* function);
    TCodegenedFunction GetCompiledMainFunction();

    bool IsCompiled();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

