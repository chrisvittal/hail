package is.hail.types.physical

import is.hail.annotations._
import is.hail.asm4s._
import is.hail.expr.ir.{EmitCodeBuilder, EmitMethodBuilder}
import is.hail.utils.FastIndexedSeq
import is.hail.variant._

object PCanonicalLocus {
  def apply(rg: ReferenceGenome): PLocus = PCanonicalLocus(rg.broadcastRG)

  def apply(rg: ReferenceGenome, required: Boolean): PLocus = PCanonicalLocus(rg.broadcastRG, required)

  private def representation(required: Boolean = false): PStruct = PCanonicalStruct(
    required,
    "contig" -> PCanonicalString(required = true),
    "position" -> PInt32(required = true))

  def schemaFromRG(rg: Option[ReferenceGenome], required: Boolean = false): PType = rg match {
    case Some(ref) => PCanonicalLocus(ref, required)
    case None => representation(required)
  }
}

final case class PCanonicalLocus(rgBc: BroadcastRG, required: Boolean = false) extends PLocus {
  def rg: ReferenceGenome = rgBc.value

  def _asIdent = "locus"

  override def _pretty(sb: StringBuilder, indent: Call, compact: Boolean): Unit = sb.append(s"PCLocus($rg)")

  def setRequired(required: Boolean) = if(required == this.required) this else PCanonicalLocus(this.rgBc, required)

  val representation: PStruct = PCanonicalLocus.representation(required)

  private[physical] def contigAddr(address: Code[Long]): Code[Long] = representation.loadField(address, 0)

  private[physical] def contigAddr(address: Long): Long = representation.loadField(address, 0)

  def contig(address: Long): String = contigType.loadString(contigAddr(address))

  lazy val contigType: PCanonicalString = representation.field("contig").typ.asInstanceOf[PCanonicalString]

  def position(off: Code[Long]): Code[Int] = Region.loadInt(representation.loadField(off, 1))

  lazy val positionType: PInt32 = representation.field("position").typ.asInstanceOf[PInt32]

  // FIXME: Remove when representation of contig/position is a naturally-ordered Long
  override def unsafeOrdering(): UnsafeOrdering = {
    val repr = representation.fundamentalType

    val localRGBc = rgBc
    val binaryOrd = repr.fieldType("contig").asInstanceOf[PBinary].unsafeOrdering()

    new UnsafeOrdering {
      def compare(o1: Long, o2: Long): Int = {
        val cOff1 = repr.loadField(o1, 0)
        val cOff2 = repr.loadField(o2, 0)

        if (binaryOrd.compare(cOff1, cOff2) == 0) {
          val posOff1 = repr.loadField(o1, 1)
          val posOff2 = repr.loadField(o2, 1)
          java.lang.Integer.compare(Region.loadInt(posOff1), Region.loadInt(posOff2))
        } else {
          val contig1 = contigType.loadString(cOff1)
          val contig2 = contigType.loadString(cOff2)
          localRGBc.value.compare(contig1, contig2)
        }
      }
    }
  }

  def codeOrdering(mb: EmitMethodBuilder[_], other: PType): CodeOrdering = {
    assert(other isOfType this)
    new CodeOrderingCompareConsistentWithOthers {
      val bincmp = representation.fundamentalType.fieldType("contig").asInstanceOf[PBinary].codeOrdering(mb)

      override def compareNonnull(cb: EmitCodeBuilder, x: PCode, y: PCode): Code[Int] = {
        val codeRG = cb.emb.getReferenceGenome(rg)
        val lhs = x.asLocus.memoize(cb, "locus_ord_lhs")
        val rhs = y.asLocus.memoize(cb, "locus_ord_rhs")
        val cmp = cb.newLocal("cmp", 0)

        val clhs = lhs.contig().asBytes()
        val crhs = rhs.contig().asBytes()

        cb.ifx(bincmp.compareNonnull(cb, clhs, crhs).ceq(0), {
          cb.assign(cmp,
            Code.invokeStatic2[java.lang.Integer, Int, Int, Int](
              "compare", lhs.position(), rhs.position()))
        }, {
          cb.assign(cmp, codeRG.invoke[String, String, Int]("compare",
            lhs.contig().loadString(),
            rhs.contig().loadString()))
        })
        cmp
      }
    }
  }
}

object PCanonicalLocusSettable {
  def apply(sb: SettableBuilder, pt: PCanonicalLocus, name: String): PCanonicalLocusSettable = {
    new PCanonicalLocusSettable(pt,
      sb.newSettable[Long](s"${ name }_a"),
      sb.newSettable[Long](s"${ name }_contig"),
      sb.newSettable[Int](s"${ name }_position"))
  }
}

class PCanonicalLocusSettable(
  val pt: PCanonicalLocus,
  val a: Settable[Long],
  _contig: Settable[Long],
  val position: Settable[Int]
) extends PLocusValue with PSettable {
  def get = new PCanonicalLocusCode(pt, a)

  def settableTuple(): IndexedSeq[Settable[_]] = FastIndexedSeq(a, _contig, position)

  def store(pc: PCode): Code[Unit] = {
    Code(
      a := pc.asInstanceOf[PCanonicalLocusCode].a,
      _contig := pt.contigAddr(a),
      position := pt.position(a))
  }

  def contig(): PStringCode = new PCanonicalStringCode(pt.contigType.asInstanceOf[PCanonicalString], _contig)
}

class PCanonicalLocusCode(val pt: PCanonicalLocus, val a: Code[Long]) extends PLocusCode {
  def code: Code[_] = a

  def codeTuple(): IndexedSeq[Code[_]] = FastIndexedSeq(a)

  def contig(): PStringCode = new PCanonicalStringCode(pt.contigType, pt.contigAddr(a))

  def position(): Code[Int] = pt.position(a)

  def getLocusObj(): Code[Locus] = {
    Code.memoize(a, "get_locus_code_memo") { a =>
      Code.invokeStatic2[Locus, String, Int, Locus]("apply",
        pt.contigType.loadString(pt.contigAddr(a)),
        pt.position(a))
    }
  }

  def memoize(cb: EmitCodeBuilder, name: String, sb: SettableBuilder): PLocusValue = {
    val s = PCanonicalLocusSettable(sb, pt, name)
    cb.assign(s, this)
    s
  }

  def memoize(cb: EmitCodeBuilder, name: String): PLocusValue = memoize(cb, name, cb.localBuilder)

  def memoizeField(cb: EmitCodeBuilder, name: String): PLocusValue = memoize(cb, name, cb.fieldBuilder)

  def store(mb: EmitMethodBuilder[_], r: Value[Region], dst: Code[Long]): Code[Unit] = Region.storeAddress(dst, a)
}
