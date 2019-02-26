"""A work in progress pipeline to combine (g)VCFs into an alternate format"""

import hail as hl
from hail.table import Table
from hail.expr import ArrayExpression, StructExpression
from hail.expr.expressions import expr_call, expr_array, expr_int32
from hail.ir import MatrixKeyRowsBy, TableKeyBy
from hail.typecheck import typecheck

# FIXME: do better with type information
transform_row_type = hl.dtype('struct{locus: locus<GRCh38>, alleles: array<str>, rsid: str, qual: float64, filters: set<str>, info: struct{BaseQRankSum: float64, ClippingRankSum: float64, DP: int32, END: int32, ExcessHet: float64, MQ: float64, MQRankSum: float64, MQ_DP: int32, QUALapprox: int32, RAW_MQ: float64, ReadPosRankSum: float64, VarDP: int32}, __entries: array<struct{AD: array<int32>, DP: int32, GQ: int32, GT: call, MIN_DP: int32, PGT: call, PID: str, PL: array<int32>, SB: array<int32>}>}')
transform_rows_f = hl.experimental.define_function(
    lambda row: hl.struct(
        alleles=row.alleles,
        rsid=row.rsid,
        info=row.info.annotate(
            SB_TABLE=hl.array([
                hl.sum(row.__entries.map(lambda d: d.SB[0])),
                hl.sum(row.__entries.map(lambda d: d.SB[1])),
                hl.sum(row.__entries.map(lambda d: d.SB[2])),
                hl.sum(row.__entries.map(lambda d: d.SB[3])),
            ])
        ).select(
            "MQ_DP",
            "QUALapprox",
            "RAW_MQ",
            "VarDP",
            "SB_TABLE",
        ),
        __entries=row.__entries.map(
            lambda e:
            hl.struct(
                BaseQRankSum=row.info['BaseQRankSum'],
                ClippingRankSum=row.info['ClippingRankSum'],
                DP=e.DP,
                END=row.info.END,
                GQ=e.GQ,
                LA=hl.range(0, hl.len(row.alleles)),
                LAD=e.AD,
                LGT=e.GT,
                LPGT=e.PGT,
                LPL=e.PL,
                MIN_DP=e.MIN_DP,
                MQ=row.info['MQ'],
                MQRankSum=row.info['MQRankSum'],
                PID=e.PID,
                ReadPosRankSum=row.info['ReadPosRankSum'],
            )
        )
    ),
    transform_row_type)

def transform_one(mt: Table) -> Table:
    """transforms a gvcf into a form suitable for combining"""
    return mt.select(**transform_rows_f(mt.row))


def merge_alleles(alleles) -> ArrayExpression:
    # alleles is tarray(tarray(tstruct(ref=tstr, alt=tstr)))
    return hl.rbind(hl.array(hl.set(hl.flatten(alleles))),
                    lambda arr:
                    hl.filter(lambda a: a.alt != '<NON_REF>', arr)
                      .extend(hl.filter(lambda a: a.alt == '<NON_REF>', arr)))


def renumber_entry(entry, old_to_new) -> StructExpression:
    # global index of alternate (non-ref) alleles
    return entry.annotate(LA=entry.LA.map(lambda lak: old_to_new[lak]))


def combine(ts):
    # pylint: disable=protected-access
    tmp = ts.annotate(
        alleles=merge_alleles(ts.data.map(lambda d: d.alleles)),
        rsid=hl.find(hl.is_defined, ts.data.map(lambda d: d.rsid)),
        info=hl.struct(
            MQ_DP=hl.sum(ts.data.map(lambda d: d.info.MQ_DP)),
            QUALapprox=hl.sum(ts.data.map(lambda d: d.info.QUALapprox)),
            RAW_MQ=hl.sum(ts.data.map(lambda d: d.info.RAW_MQ)),
            VarDP=hl.sum(ts.data.map(lambda d: d.info.VarDP)),
            SB_TABLE=hl.array([
                hl.sum(ts.data.map(lambda d: d.info.SB_TABLE[0])),
                hl.sum(ts.data.map(lambda d: d.info.SB_TABLE[1])),
                hl.sum(ts.data.map(lambda d: d.info.SB_TABLE[2])),
                hl.sum(ts.data.map(lambda d: d.info.SB_TABLE[3]))
            ])))
    tmp = tmp.annotate(
        __entries=hl.bind(
            lambda combined_allele_index:
            hl.range(0, hl.len(tmp.data)).flatmap(
                lambda i:
                hl.cond(hl.is_missing(tmp.data[i].__entries),
                        hl.range(0, hl.len(tmp.g[i].__cols))
                          .map(lambda _: hl.null(tmp.data[i].__entries.dtype.element_type)),
                        hl.bind(
                            lambda old_to_new: tmp.data[i].__entries.map(lambda e: renumber_entry(e, old_to_new)),
                            hl.array([0]).extend(
                                hl.range(0, hl.len(tmp.data[i].alleles)).map(
                                    lambda j: combined_allele_index[tmp.data[i].alleles[j]]))))),
            hl.dict(hl.range(1, hl.len(tmp.alleles) + 1).map(
                lambda j: hl.tuple([tmp.alleles[j - 1], j])))))
    tmp = tmp.annotate_globals(__cols=hl.flatten(tmp.g.map(lambda g: g.__cols)))

    return tmp.drop('data', 'g')


def combine_gvcfs(mts):
    """merges vcfs using multi way join"""

    # pylint: disable=protected-access
    def localize(mt):
        return mt._localize_entries('__entries', '__cols')

    def fix_alleles(alleles):
        return hl.rbind(
            alleles.map(lambda d: d.ref).fold(lambda s, t: hl.cond(hl.len(s) > hl.len(t), s, t), ''),
            lambda ref: hl.rbind(
                alleles.map(lambda a: hl.switch(hl.allele_type(a.ref, a.alt))
                                        .when('SNP', a.alt + ref[hl.len(a.alt):])
                                        .when('Insertion', a.alt + ref[hl.len(a.ref):])
                                        .when('Deletion', a.alt + ref[hl.len(a.ref):])
                                        .default(a.alt)),
                lambda alts: hl.array([ref]).extend(alts)
            ))

    def min_rep(locus, ref, alt):
        return hl.rbind(hl.min_rep(locus, [ref, alt]),
                        lambda mr: hl.case()
                          .when(alt == '<NON_REF>', hl.struct(ref=ref[0:1], alt=alt))
                          .when(locus == mr.locus, hl.struct(ref=mr.alleles[0], alt=mr.alleles[1]))
                          .or_error("locus before and after minrep differ"))

    mts = [mt.annotate(
        # now minrep'ed (ref, alt) allele pairs
        alleles=hl.bind(lambda ref, locus: mt.alleles[1:].map(lambda alt: min_rep(locus, ref, alt)),
                        mt.alleles[0], mt.locus)) for mt in mts]
    ts = hl.Table._multi_way_zip_join([(mt) for mt in mts], 'data', 'g')
    combined = combine(ts)
    combined = combined.annotate(alleles=fix_alleles(combined.alleles))
    return combined._unlocalize_entries('__entries', '__cols', ['s'])


@typecheck(lgt=expr_call, la=expr_array(expr_int32))
def lgt_to_gt(lgt, la):
    """A method for transforming Local GT and Local Alleles into the true GT"""
    return hl.call(la[lgt[0]], la[lgt[1]])


def summarize(mt):
    """Computes summary statistics

    Note
    ----
    You will not be able to run :func:`.combine_gvcfs` with the output of this
    function.
    """
    mt = hl.experimental.densify(mt)
    return mt.annotate_rows(info=hl.rbind(
        hl.agg.call_stats(lgt_to_gt(mt.LGT, mt.LA), mt.alleles),
        lambda gs: hl.struct(
            # here, we alphabetize the INFO fields by GATK convention
            AC=gs.AC,
            AF=gs.AF,
            AN=gs.AN,
            BaseQRankSum=hl.median(hl.agg.collect(mt.entry.BaseQRankSum)),
            ClippingRankSum=hl.median(hl.agg.collect(mt.entry.ClippingRankSum)),
            DP=hl.agg.sum(mt.entry.DP),
            MQ=hl.median(hl.agg.collect(mt.entry.MQ)),
            MQRankSum=hl.median(hl.agg.collect(mt.entry.MQRankSum)),
            MQ_DP=mt.info.MQ_DP,
            QUALapprox=mt.info.QUALapprox,
            RAW_MQ=mt.info.RAW_MQ,
            ReadPosRankSum=hl.median(hl.agg.collect(mt.entry.ReadPosRankSum)),
            SB_TABLE=mt.info.SB_TABLE,
            VarDP=mt.info.VarDP,
        )))

def finalize(mt):
    """Drops entry fields no longer needed for combining.

    Note
    ----
    You will not be able to run :func:`.combine_gvcfs` with the output of this
    function.
    """
    return mt.drop('BaseQRankSum', 'ClippingRankSum', 'MQ', 'MQRankSum', 'ReadPosRankSum')


def reannotate(mt, gatk_ht, summarize_ht):
    """Re-annotate a sparse MT with annotations from certain GATK tools

    `gatk_ht` should be a table from the rows of a VCF, with `info` having at least
    the following fields.  Be aware that fields not present in this list will
    be dropped.
    ```
        struct {
            AC: array<int32>,
            AF: array<float64>,
            AN: int32,
            BaseQRankSum: float64,
            ClippingRankSum: float64,
            DP: int32,
            FS: float64,
            MQ: float64,
            MQRankSum: float64,
            MQ_DP: int32,
            NEGATIVE_TRAIN_SITE: bool,
            POSITIVE_TRAIN_SITE: bool,
            QD: float64,
            QUALapprox: int32,
            RAW_MQ: float64,
            ReadPosRankSum: float64,
            SB_TABLE: array<int32>,
            SOR: float64,
            VQSLOD: float64,
            VarDP: int32,
            culprit: str
        }
    ```
    `summarize_ht` should be the output of :func:`.summarize` as a rows table.

    Note
    ----
    You will not be able to run :func:`.combine_gvcfs` with the output of this
    function.
    """
    gatk_ht = hl.Table(TableKeyBy(gatk_ht._tir, ['locus'], is_sorted=True))
    summ_ht = hl.Table(TableKeyBy(summarize_ht._tir, ['locus'], is_sorted=True))
    return mt.annotate_rows(
        info=hl.rbind(
            gatk_ht[mt.locus].info, summ_ht[mt.locus].info,
            lambda ginfo, hinfo: hl.struct(
                AC=hl.or_else(hinfo.AC, ginfo.AC),
                AF=hl.or_else(hinfo.AF, ginfo.AF),
                AN=hl.or_else(hinfo.AN, ginfo.AN),
                BaseQRankSum=hl.or_else(hinfo.BaseQRankSum, ginfo.BaseQRankSum),
                ClippingRankSum=hl.or_else(hinfo.ClippingRankSum, ginfo.ClippingRankSum),
                DP=hl.or_else(hinfo.DP, ginfo.DP),
                FS=ginfo.FS,
                MQ=hl.or_else(hinfo.MQ, ginfo.MQ),
                MQRankSum=hl.or_else(hinfo.MQRankSum, ginfo.MQRankSum),
                MQ_DP=hl.or_else(hinfo.MQ_DP, ginfo.MQ_DP),
                NEGATIVE_TRAIN_SITE=ginfo.NEGATIVE_TRAIN_SITE,
                POSITIVE_TRAIN_SITE=ginfo.POSITIVE_TRAIN_SITE,
                QD=ginfo.QD,
                QUALapprox=hl.or_else(hinfo.QUALapprox, ginfo.QUALapprox),
                RAW_MQ=hl.or_else(hinfo.RAW_MQ, ginfo.RAW_MQ),
                ReadPosRankSum=hl.or_else(hinfo.ReadPosRankSum, ginfo.ReadPosRankSum),
                SB_TABLE=hl.or_else(hinfo.SB_TABLE, ginfo.SB_TABLE),
                SOR=ginfo.SOR,
                VQSLOD=ginfo.VQSLOD,
                VarDP=hl.or_else(hinfo.VarDP, ginfo.VarDP),
                culprit=ginfo.culprit,
            )),
        qual=gatk_ht[mt.locus].qual,
        filters=gatk_ht[mt.locus].filters,
    )


# NOTE: these are just @chrisvittal's notes on how gVCF fields are combined
#       some of it is copied from GenomicsDB's wiki.
# always missing items include MQ, HaplotypeScore, InbreedingCoeff
# items that are dropped by CombineGVCFs and so set to missing are MLEAC, MLEAF
# Notes on info aggregation, The GenomicsDB wiki says the following:
#   The following operations are supported:
#       "sum" sum over valid inputs
#       "mean"
#       "median"
#       "element_wise_sum"
#       "concatenate"
#       "move_to_FORMAT"
#       "combine_histogram"
#
#   Operations for the fields
#   QUAL: set to missing
#   INFO {
#       BaseQRankSum: median, # NOTE : move to format for combine
#       ClippingRankSum: median, # NOTE : move to format for combine
#       DP: sum
#       ExcessHet: median, # NOTE : this can also be dropped
#       MQ: median, # NOTE : move to format for combine
#       MQ_DP: sum,
#       MQ0: median,
#       MQRankSum: median, # NOTE : move to format for combine
#       QUALApprox: sum,
#       RAW_MQ: sum
#       ReadPosRankSum: median, # NOTE : move to format for combine
#       SB_TABLE: elementwise sum, # NOTE: after being moved from FORMAT as SB
#       VarDP: sum
#   }
#   FORMAT {
#       END: move from INFO
#   }
#
# The following are Truncated INFO fields for the specific VCFs this tool targets
# ##INFO=<ID=BaseQRankSum,Number=1,Type=Float>
# ##INFO=<ID=ClippingRankSum,Number=1,Type=Float>
# ##INFO=<ID=DP,Number=1,Type=Integer>
# ##INFO=<ID=END,Number=1,Type=Integer>
# ##INFO=<ID=ExcessHet,Number=1,Type=Float>
# ##INFO=<ID=MQ,Number=1,Type=Float>
# ##INFO=<ID=MQRankSum,Number=1,Type=Float>
# ##INFO=<ID=MQ_DP,Number=1,Type=Integer>
# ##INFO=<ID=QUALapprox,Number=1,Type=Integer>
# ##INFO=<ID=RAW_MQ,Number=1,Type=Float>
# ##INFO=<ID=ReadPosRankSum,Number=1,Type=Float>
# ##INFO=<ID=VarDP,Number=1,Type=Integer>
#
# As of 2/15/19, the schema returned by the combiner is as follows:
# ----------------------------------------
# Global fields:
#     None
# ----------------------------------------
# Column fields:
#     's': str
# ----------------------------------------
# Row fields:
#     'locus': locus<GRCh38>
#     'alleles': array<str>
#     'rsid': str
#     'info': struct {
#         MQ_DP: int32,
#         QUALapprox: int32,
#         RAW_MQ: float64,
#         VarDP: int32,
#         SB_TABLE: array<int64>
#     }
# ----------------------------------------
# Entry fields:
#     'LAD': array<int32>
#     'DP': int32
#     'GQ': int32
#     'LGT': call
#     'MIN_DP': int32
#     'LPGT': call
#     'PID': str
#     'LPL': array<int32>
#     'LA': array<int32>
#     'END': int32
#     'BaseQRankSum': float64
#     'ClippingRankSum': float64
#     'MQ': float64
#     'MQRankSum': float64
#     'ReadPosRankSum': float64
# ----------------------------------------
# Column key: ['s']
# Row key: ['locus', 'alleles']
# ----------------------------------------
