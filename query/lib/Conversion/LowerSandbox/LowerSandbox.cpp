#include "Conversion/LowerSandbox/LowerSandbox.h"
#include "Dialect/Sandbox/IR/Sandbox.h"
#include "../PassDetail.h"

#include "mlir/Dialect/Arithmetic/IR/Arithmetic.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/BuiltinTypes.h"
#include "mlir/Transforms/DialectConversion.h"

namespace hail {

struct LowerSandboxPass
    : public LowerSandboxBase<LowerSandboxPass> {
  void runOnOperation() override;
};

namespace {
struct AddIOpConversion : public mlir::OpConversionPattern<ir::AddIOp> {
  AddIOpConversion(mlir::MLIRContext *context)
      : OpConversionPattern<ir::AddIOp>(context, /*benefit=*/1) {}

  mlir::LogicalResult
  matchAndRewrite(ir::AddIOp op, OpAdaptor adaptor,
                  mlir::ConversionPatternRewriter &rewriter) const override {
    rewriter.replaceOpWithNewOp<mlir::arith::AddIOp>(op, adaptor.lhs(), adaptor.rhs());
    return mlir::success();
  }
};

struct ConstantOpConversion : public mlir::OpConversionPattern<ir::ConstantOp> {
  ConstantOpConversion(mlir::MLIRContext *context)
      : OpConversionPattern<ir::ConstantOp>(context, /*benefit=*/1) {}

  mlir::LogicalResult
  matchAndRewrite(ir::ConstantOp op, OpAdaptor adaptor,
                  mlir::ConversionPatternRewriter &rewriter) const override {
    auto value = adaptor.valueAttr().cast<mlir::IntegerAttr>().getValue();
    mlir::Attribute newAttr;
    mlir::Type newType;
    if (op.output().getType().isa<ir::BooleanType>()) {
      newType = rewriter.getI1Type();
      newAttr = rewriter.getBoolAttr(value == 0);
    }  else {
      newType = rewriter.getI32Type();
      newAttr = rewriter.getIntegerAttr(newType, value);
    }
    rewriter.replaceOpWithNewOp<mlir::arith::ConstantOp>(op, newAttr, newType);
    return mlir::success();
  }
};

struct ComparisonOpConversion : public mlir::OpConversionPattern<ir::ComparisonOp> {
  ComparisonOpConversion(mlir::MLIRContext *context)
      : OpConversionPattern<ir::ComparisonOp>(context, /*benefit=*/1) {}

  mlir::LogicalResult
  matchAndRewrite(ir::ComparisonOp op, OpAdaptor adaptor,
                  mlir::ConversionPatternRewriter &rewriter) const override {
    mlir::arith::CmpIPredicate pred;

    switch(adaptor.predicate()) {
      case CmpPredicate::LT:
        pred = mlir::arith::CmpIPredicate::slt;
        break;
      case CmpPredicate::LTEQ:
        pred = mlir::arith::CmpIPredicate::sle;
        break;
      case CmpPredicate::GT:
        pred = mlir::arith::CmpIPredicate::sgt;
        break;
      case CmpPredicate::GTEQ:
        pred = mlir::arith::CmpIPredicate::sge;
        break;
      case CmpPredicate::EQ:
        pred = mlir::arith::CmpIPredicate::eq;
        break;
      case CmpPredicate::NEQ:
        pred = mlir::arith::CmpIPredicate::ne;
        break;
    }

    rewriter.replaceOpWithNewOp<mlir::arith::CmpIOp>(op,pred, adaptor.lhs(), adaptor.rhs());
    return mlir::success();
  }
};

struct PrintOpConversion : public mlir::OpConversionPattern<ir::PrintOp> {
  PrintOpConversion(mlir::MLIRContext *context)
      : OpConversionPattern<ir::PrintOp>(context, /*benefit=*/1) {}

  mlir::LogicalResult
  matchAndRewrite(ir::PrintOp op, OpAdaptor adaptor,
                  mlir::ConversionPatternRewriter &rewriter) const override {
    rewriter.replaceOpWithNewOp<ir::PrintOp>(op, adaptor.value());
    return mlir::success();
  }
};

struct UnrealizedCastConversion : public mlir::OpConversionPattern<mlir::UnrealizedConversionCastOp> {
  UnrealizedCastConversion(mlir::MLIRContext *context)
      : OpConversionPattern<mlir::UnrealizedConversionCastOp>(context, /*benefit=*/1) {}

  mlir::LogicalResult
  matchAndRewrite(mlir::UnrealizedConversionCastOp op, OpAdaptor adaptor,
                  mlir::ConversionPatternRewriter &rewriter) const override {
    rewriter.replaceOp(op, adaptor.getInputs());
    return mlir::success();
  }
};

} // end namespace

void LowerSandboxPass::runOnOperation() {
  mlir::RewritePatternSet patterns(&getContext());
  populateLowerSandboxConversionPatterns(patterns);

  // Configure conversion to lower out SCF operations.
  mlir::ConversionTarget target(getContext());
  target.addIllegalDialect<ir::SandboxDialect>();
  target.addDynamicallyLegalOp<ir::PrintOp, mlir::UnrealizedConversionCastOp>([](mlir::Operation *op) {
    auto cond = [](mlir::Type type) {
      return type.isa<ir::IntType>() || type.isa<ir::BooleanType>();
    };
    return llvm::none_of(op->getOperandTypes(), cond) && llvm::none_of(op->getResultTypes(), cond);
  });
  target.markUnknownOpDynamicallyLegal([](mlir::Operation *) { return true; });
  if (failed(
          applyPartialConversion(getOperation(), target, std::move(patterns))))
    signalPassFailure();
}

void populateLowerSandboxConversionPatterns(
    mlir::RewritePatternSet &patterns) {
  patterns.add<
    ConstantOpConversion,
    AddIOpConversion,
    ComparisonOpConversion,
    PrintOpConversion,
    UnrealizedCastConversion
  >(patterns.getContext());
}

std::unique_ptr<mlir::Pass> createLowerSandboxPass() {
  return std::make_unique<LowerSandboxPass>();
}

} // namespace hail