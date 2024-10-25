// #include "llvm/Analysis/Verifier.h"
// #include "llvm/Assembly/PrintModulePass.h"
// #include "llvm/CallingConv.h"
// #include "llvm/Function.h"
#include "llvm-c/Disassembler.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/CallingConv.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/IRPrintingPasses.h"
#include "llvm/IR/Instruction.h"
#include "llvm/IR/Instructions.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Verifier.h"
// #include "llvm/IR/Analysis.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/VectorBuilder.h"
#include "llvm/IR/PassManager.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/TargetParser/Host.h"
// #include "llvm/IR/raw_ostream.h"
#include "yt/yt/library/codegen/module.h"
#include "yt/yt/library/query/engine/cg_fragment_compiler.h"
#include "yt/yt/library/query/engine/cg_helpers.h"
#include "yt/yt/library/query/engine/position_independent_value_caller.h"
#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/IR/IntrinsicInst.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/Module.h>
#include <llvm/Support/X86DisassemblerDecoderCommon.h>
#include <stdexcept>

// #include "llvm/PassManager.h"
// #include "llvm/Support/IRBuilder.h"
#include "llvm/Support/raw_ostream.h"

std::unique_ptr<llvm::Module> makeLLVMModule(llvm::LLVMContext &ctx) {
  return std::make_unique<llvm::Module>("test", ctx);
}

/*
 * Disassemble a function, using the LLVM MC disassembler.
 *
 * See also:
 * - http://blog.llvm.org/2010/01/x86-disassembler.html
 * - http://blog.llvm.org/2010/04/intro-to-llvm-mc-project.html
 */
static size_t disassemble(const void *func, std::ostream &buffer) {
  const uint8_t *bytes = (const uint8_t *)func;

  /*
   * Limit disassembly to this extent
   */
  const uint64_t extent = 96 * 1024;

  /*
   * Initialize all used objects.
   */

  const char *triple = LLVM_HOST_TRIPLE;
  //  llvm::InitializeNativeTarget();
  //  llvm::InitializeMachine
  LLVMInitializeAllTargetInfos();
  LLVMInitializeAllTargetMCs();
  LLVMInitializeAllDisassemblers();
  LLVMDisasmContextRef D = LLVMCreateDisasm(triple, NULL, 0, NULL, NULL);
  char outline[1024];

  if (!D) {
    buffer << "error: could not create disassembler for triple " << triple
           << '\n';
    return 0;
  }

  uint64_t pc;
  pc = 0;
  while (pc < extent) {
    size_t Size;

    /*
     * Print address.  We use addresses relative to the start of the function,
     * so that between runs.
     */

    buffer << std::setw(6) << std::hex << (unsigned long)pc << std::setw(0)
           << std::dec << ":";

    Size = LLVMDisasmInstruction(D, (uint8_t *)bytes + pc, extent - pc, 0,
                                 outline, sizeof(outline));

    if (!Size) {
#if DETECT_ARCH_AARCH64
      uint32_t invalid = bytes[pc + 0] << 0 | bytes[pc + 1] << 8 |
                         bytes[pc + 2] << 16 | bytes[pc + 3] << 24;
      snprintf(outline, sizeof(outline), "\tinvalid %x", invalid);
      Size = 4;
#else
      buffer << "\tinvalid\n";
      break;
#endif
    }

    /*
     * Output the bytes in hexidecimal format.
     */

    if (0) {
      unsigned i;
      for (i = 0; i < Size; ++i) {
        buffer << std::hex << std::setfill('0') << std::setw(2)
               << static_cast<int>(bytes[pc + i]) << std::setw(0) << std::dec;
      }
      for (; i < 16; ++i) {
        buffer << "   ";
      }
    }

    /*
     * Print the instruction.
     */

    buffer << outline << '\n';

    /*
     * Advance.
     */

    pc += Size;

    /*
     * Stop disassembling on return statements
     */

#if DETECT_ARCH_X86 || DETECT_ARCH_X86_64
    if (Size == 1 && bytes[pc - 1] == 0xc3) {
      break;
    }
#elif DETECT_ARCH_AARCH64
    if (Size == 4 && bytes[pc - 1] == 0xD6 && bytes[pc - 2] == 0x5F &&
        (bytes[pc - 3] & 0xFC) == 0 && (bytes[pc - 4] & 0x1F) == 0) {
      break;
    }
#endif

    if (pc >= extent) {
      buffer << "disassembly larger than " << extent << " bytes, aborting\n";
      break;
    }
  }

  buffer << '\n';

  LLVMDisasmDispose(D);

  /*
   * Print GDB command, useful to verify output.
   */
  if (0) {
    buffer << "disassemble " << std::hex << static_cast<const void *>(bytes)
           << ' ' << static_cast<const void *>(bytes + pc) << std::dec << '\n';
  }

  return pc;
}

void generate_mul_add_scalar(llvm::LLVMContext &ctx, llvm::Function *mul_add) {
  using namespace llvm;
  mul_add->setCallingConv(CallingConv::C);

  IRBuilder<> builder(ctx);

  BasicBlock *start = BasicBlock::Create(ctx, "start", mul_add);
  BasicBlock *ret = BasicBlock::Create(ctx, "ret", mul_add);
  BasicBlock *loop = BasicBlock::Create(ctx, "loop", mul_add);

  // index_ptr = 0;
  Function::arg_iterator args = mul_add->arg_begin();
  Value *data_ptr = args++;
  data_ptr->setName("dataPtr");
  Value *data_size = args++;
  data_size->setName("dataSize");

  builder.SetInsertPoint(start);
  Value *index_ptr =
      builder.CreateAlloca(builder.getInt32Ty(), nullptr, "indexPtr");
  Value *sum_ptr =
      builder.CreateAlloca(builder.getInt32Ty(), nullptr, "sumPtr");
  llvm::Value *ZERO_CONST =
      llvm::ConstantInt::get(ctx, llvm::APInt(32, 0, true));
  llvm::Value *ONE_CONST =
      llvm::ConstantInt::get(ctx, llvm::APInt(32, 1, true));

  builder.CreateStore(ZERO_CONST, sum_ptr);
  builder.CreateStore(ZERO_CONST, index_ptr);
  builder.CreateBr(loop);

  builder.SetInsertPoint(loop);
  Value *index = builder.CreateLoad(builder.getInt32Ty(), index_ptr, "index");
  Value *sum = builder.CreateLoad(builder.getInt32Ty(), sum_ptr, "sum");
  Value *data_el_ptr = builder.CreateGEP(builder.getInt32Ty(), data_ptr, index);
  Value *data_el =
      builder.CreateLoad(builder.getInt32Ty(), data_el_ptr, "data_el");

  Value *new_cur_sum =
      builder.CreateBinOp(Instruction::Add, sum, data_el, "new_sum");

  Value *new_index =
      builder.CreateBinOp(Instruction::Add, index, ONE_CONST, "new_index");

  builder.CreateStore(new_cur_sum, sum_ptr);
  builder.CreateStore(new_index, index_ptr);
  Value *cmp_res = builder.CreateICmpSLT(new_index, data_size);
  builder.CreateCondBr(cmp_res, loop, ret);

  builder.SetInsertPoint(ret);
  builder.CreateRet(builder.CreateLoad(builder.getInt32Ty(), sum_ptr, "sum"));
}

void generate_mul_add_vector(llvm::LLVMContext &ctx, llvm::Function *mul_add) {
  using namespace llvm;
  mul_add->setCallingConv(CallingConv::C);

  IRBuilder<> builder(ctx);

  // BasicBlock *start = BasicBlock::Create(ctx, "start", mul_add);
  BasicBlock *ret = BasicBlock::Create(ctx, "ret", mul_add);
  // BasicBlock *loop = BasicBlock::Create(ctx, "loop", mul_add);

  // index_ptr = 0;
  Function::arg_iterator args = mul_add->arg_begin();
  Value *data_ptr = args++;
  data_ptr->setName("dataPtr");
  Value *data_size = args++;
  data_size->setName("dataSize");

  builder.SetInsertPoint(ret);
  auto vector_builder =
      llvm::VectorBuilder(builder, VectorBuilder::Behavior::ReportAndAbort);

  // Value* vec = UndefValue::get(VectorType::get(builder.getInt32Ty(), ElementCount::getScalable(4)));
  // InsertElementInst::Create(
  //     vec, Constant::getIntegerValue(builder.getInt32Ty(), llvm::APInt(32, 0)),
  //     Constant::getIntegerValue(builder.getInt32Ty(), llvm::APInt(32, 0)));
  // InsertElementInst::Create(
  //     vec, Constant::getIntegerValue(builder.getInt32Ty(), llvm::APInt(32, 1)),
  //     Constant::getIntegerValue(builder.getInt32Ty(), llvm::APInt(32, 1)));
  //  InsertElementInst::Create(
  //     vec, Constant::getIntegerValue(builder.getInt32Ty(), llvm::APInt(32, 2)),
  //     Constant::getIntegerValue(builder.getInt32Ty(), llvm::APInt(32, 2)));
  //  InsertElementInst::Create(
  //      vec, Constant::getIntegerValue(builder.getInt32Ty(), llvm::APInt(32, 3)),
  //      Constant::getIntegerValue(builder.getInt32Ty(), llvm::APInt(32, 3)));


  //  ExtractElementInst::Create(vec, Constant::getIntegerValue(builder.getInt32Ty(), llvm::APInt(32, 3)));
   VectorType *vector_type =
       VectorType::get(builder.getInt32Ty(), 4, false);
   //  auto *IntVecTy =
   //      FixedVectorType::get(Type::getInt32Ty(ctx), 4);
  //  Function *rcpps =
  //      Intrinsic::getDeclaration(mod, Intrinsic::x86_sse_rcp_ps);

  //  Value *out = builder->CreateCall(rcpps, in);

  //  builder->Insert(new StoreInst(out, out_arg, false, 1));

   Value* vector_res = builder.CreateLoad(vector_type, data_ptr, "vector load");
   vector_res->dump();
      Value* vector_res2 = builder.CreateLoad(vector_type, data_ptr, "vector load");
   vector_res2->dump();
   Value *vector_res3 = builder.CreateBinOp(Instruction::Add, vector_res,
                                              vector_res2, "new_index");
   vector_res3->dump();
   builder.CreateRet(builder.CreateBinOp(
       Instruction::Add,
       builder.CreateExtractElement(
           vector_res3,
           Constant::getIntegerValue(builder.getInt32Ty(), llvm::APInt(32, 0))),
       builder.CreateExtractElement(
           vector_res3, Constant::getIntegerValue(builder.getInt32Ty(),
                                                  llvm::APInt(32, 1)))));

   //  auto *LLVMLd = cast<llvm::LoadInst>(res);
   //  res->dump();

   //  [[maybe_unused]] Value *sm = vector_builder.createVectorInstruction(
   //      Instruction::Add, vector_type, {vector_res, vector_res2},
   //      "vp.op.load");
   //   sm->dump();

   // Function *fun = Intrinsic::getDeclaration(F.getParent(), Intrinsic,
   // arg_type);
   // IRBuilder<> Builder(&I);
   // Builder.CreateCall(fun, args);

   //  auto res = builder.CreateExtractElement(
   //      sm, Constant::getIntegerValue(builder.getInt32Ty(),
   //      llvm::APInt(32, 0)));

   //  res->dump();
   //  ExtractElementInst::Create(
   //      vector_res,
   //      Constant::getIntegerValue(builder.getInt32Ty(), llvm::APInt(32,
   //      0)), "extract element");

  //  builder.CreateRet(
  //      Constant::getIntegerValue(builder.getInt32Ty(), llvm::APInt(32, 2)));
}

int llvm_main() {
  using namespace llvm;
  llvm::LLVMContext ctx;
  std::unique_ptr<llvm::Module> mod = makeLLVMModule(ctx);
  [[maybe_unused]] FunctionCallee c =
      mod->getOrInsertFunction("mul_add",
                               /*ret type*/ IntegerType::get(ctx, 32),
                               /*args*/
                               PointerType::get(ctx, 64), // ptr
                               IntegerType::get(ctx, 32)  // size
                               /*varargs terminated with null*/);
  llvm::Function *mul_add = cast<Function>(c.getCallee());
  generate_mul_add_vector(ctx, mul_add);
  InitializeNativeTarget();
  InitializeNativeTargetAsmPrinter();
  InitializeNativeTargetAsmParser();
  auto triple = llvm::Triple::normalize(llvm::sys::getProcessTriple());
  mod->setTargetTriple(triple);
  mod->dump();
  std::string err;
  llvm::EngineBuilder EB(std::move(mod));
  EB.setEngineKind(llvm::EngineKind::JIT)
      .setOptLevel(llvm::CodeGenOpt::None)
      .setErrorStr(&err);
  llvm::ExecutionEngine *EE = EB.create();
  if (!EE) {
    throw std::runtime_error(err);
  }
  EE->finalizeObject();
  const auto fa = (int (*)(int *, int))EE->getFunctionAddress("mul_add");
  disassemble((void *)EE->getFunctionAddress("mul_add"), std::cout);
  int x[5] = {1, 2, 3, 4, 5};
  int sz = 5;
  std::cout << "res=" << fa(x, sz) << std::endl;

  return 0;
}

void sum_fun() {}
// int llvm_main_2() {
//   NYT::NCodegen::TRoutineRegistry NativeRegistry;
//   NativeRegistry.RegisterRoutine("MulAddModule", &sum_fun);
//   NYT::NCodegen::TCGModulePtr module = NYT::NCodegen::TCGModule::Create(
//       &NativeRegistry, NYT::NCodegen::EExecutionBackend::Native,
//       "MulAddModule");
//   using sum_add_signature = int32_t(int32_t, int32_t, int32_t);

//   [[maybe_unused]] auto *mulAddFun =
//       NYT::NQueryClient::MakeFunction<sum_add_signature>(
//           module, "mul_add",
//           [&](NYT::NQueryClient::TCGBaseContext &builder,
//               [[maybe_unused]] llvm::Value *a, [[maybe_unused]] llvm::Value
//               *b,
//               [[maybe_unused]] llvm::Value *c) {
//             llvm::Value *tmp = builder->CreateBinOp(llvm::Instruction::Mul,
//             a, b, "tmp"); llvm::Value *tmp2 =
//             builder->CreateBinOp(llvm::Instruction::Add, tmp, c, "tmp2");
//             builder->CreateRet(tmp2);
//           });
//   module->ExportSymbol("SomeModule");
//   module->GetModule()->dump();
//   auto piFunction =
//   module->GetCompiledFunction<sum_add_signature>("mul_add");
//   [[maybe_unused]] auto caller = New<
//   NYT::NQueryClient::TCGPICaller<sum_add_signature,
//   sum_add_signature>>(piFunction);
//   // auto *staticInvoke =
//   //     &NYT::NQueryClient::TCGPICaller<sum_add_signature,
//   sum_add_signature>::StaticInvoke;
//   // auto cg_entrypoint = BuildCGEntrypoint<sum_add_signature,
//   sum_add_signature>(
//   //     module, "mul_add", NYT::NCodegen::EExecutionBackend::Native);
//   // // // auto image = BuildImage(module,
//   NYT::NCodegen::EExecutionBackend::Native);
//   // auto res = piFunction(1, 2, 3);
//   std::cout << res << std::endl;
//   return 0;
// }

int main() {
  llvm_main();
  // llvm_main_2();
}
