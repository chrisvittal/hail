package is.hail.backend

import is.hail.annotations.{Region, SafeRow}
import is.hail.backend.spark.SparkBackend
import is.hail.{HailContext, cxx}
import is.hail.expr.JSONAnnotationImpex
import is.hail.expr.ir.{Compilable, Compile, CompileAndEvaluate, IR, MakeTuple, Pretty}
import is.hail.expr.types.physical.PTuple
import is.hail.expr.types.virtual.TVoid
import is.hail.utils._
import org.json4s.DefaultFormats
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.reflect.ClassTag

abstract class BroadcastValue[T] { def value: T }

abstract class Backend {
  def broadcast[T: ClassTag](value: T): BroadcastValue[T]

  def parallelizeAndComputeWithIndex[T: ClassTag, U : ClassTag](collection: Array[T])(f: (T, Int) => U): Array[U]

  def lower(ir: IR, timer: Option[ExecutionTimer], optimize: Boolean = true): IR =
      LowerTableIR(ir, timer, optimize)

  def jvmLowerAndExecute(ir0: IR, optimize: Boolean): (Any, Timings) = {
    val timer = new ExecutionTimer("Backend.execute")
    val ir = lower(ir0, Some(timer), optimize)

    if (!Compilable(ir))
      throw new LowererUnsupportedOperation(s"lowered to uncompilable IR: ${Pretty(ir)}")

    val res = Region.scoped { region =>
      ir.typ match {
        case TVoid =>
          val (_, f) = timer.time(Compile[Unit](ir), "JVM compile")
          timer.time(f(0)(region), "Runtime")
        case _ =>
          val (pt: PTuple, f) = timer.time(Compile[Long](MakeTuple(FastSeq(ir))), "JVM compile")
          timer.time(SafeRow(pt, region, f(0)(region)).get(0), "Runtime")
      }
    }

    (res, timer.timings)
  }

  def cxxLowerAndExecute(ir0: IR, optimize: Boolean): (Any, Timings) =
    throw new cxx.CXXUnsupportedOperation("can't execute C++ on non-Spark backend!")

  def execute(ir: IR, optimize: Boolean): (Any, Timings) = {
    try {
      if (HailContext.get.flags.get("cpp") == null) {
        if (HailContext.get.flags.get("lower") == null)
          throw new LowererUnsupportedOperation("lowering not enabled")
        jvmLowerAndExecute(ir, optimize)
      }
      else
        cxxLowerAndExecute(ir, optimize)
    } catch {
      case (_: cxx.CXXUnsupportedOperation | _: LowererUnsupportedOperation) =>
        CompileAndEvaluate(ir, optimize = optimize)
    }
  }

  def executeJSON(ir: IR): String = {
    val t = ir.typ
    val (value, timings) = execute(ir, optimize = true)
    val jsonValue = JsonMethods.compact(JSONAnnotationImpex.exportAnnotation(value, t))
    timings.logInfo()

    Serialization.write(Map("value" -> jsonValue, "timings" -> timings.value))(new DefaultFormats {})
  }

  def asSpark(): SparkBackend = fatal("SparkBackend needed for this operation.")
}
