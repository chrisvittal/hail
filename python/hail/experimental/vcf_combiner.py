"""A work in progress pipeline to combine (g)VCFs into an alternate format"""

import operator

import hail as hl
from hail.matrixtable import MatrixTable
from hail.table import Table
from hail.expr import ArrayExpression, Expression, NumericExpression, StructExpression

REF_CALL = hl.call(0, 0, phased=False)


def mappend(fun, left, right) -> Expression:
    return hl.case()\
             .when(hl.is_missing(left), right)\
             .when(hl.is_missing(right), left)\
             .default(fun(left, right))


def position(arr, pred) -> NumericExpression:
    """gets the offset of the first element in arr that returns true for pred"""
    def helper(acc, elem):
        return hl.case(missing_false=True)\
                 .when(acc.found, acc)\
                 .when(pred(elem), acc.annotate(found=True))\
                 .default(acc.annotate(ind=acc.ind + 1))
    res = arr.fold(hl.struct(ind=0, found=False), helper)
    return hl.case().when(res.found, res.ind).default(hl.null(hl.tint32))


def read_and_transform_one(path, reference_genome='GRCh38') -> MatrixTable:
    """reads and transforms a gvcf into a form suitable for combining"""
    def new_array(expr):
        return hl.empty_array(expr.dtype).append(expr)
    mt = hl.import_vcf(path, reference_genome=reference_genome)
    mt = mt.annotate_entries(END=mt.info.END, PL=mt['PL'][0:])
    # This collects all fields with median combiners into arrays so we can calculate medians
    # when needed
    mt = mt.annotate_rows(info=mt.info.annotate(
        BaseQRankSum=new_array(mt.info['BaseQRankSum']),
        ClippingRankSum=new_array(mt.info['ClippingRankSum']),
        MQ=new_array(mt.info['MQ']),
        MQRankSum=new_array(mt.info['MQRankSum']),
        ReadPosRankSum=new_array(mt.info['MQRankSum']),
        SB=hl.agg.array_sum(mt.entry.SB)).drop('END'))
    # mt = mt.annotate_entries(LA=hl.range(0, mt.alleles.length()))
    # NOTE until joins are improved, we only key by locus for now
    return mt.drop('SB', 'qual').key_rows_by('locus')


def combine_infos(info_left, info_right) -> StructExpression:
    """combines transformed fields"""
    def ext(lft, rgt):
        return lft.extend(rgt)
    return hl.struct(
        BaseQRankSum=mappend(ext, info_left['BaseQRankSum'], info_right['BaseQRankSum']),
        ClippingRankSum=mappend(ext, info_left['ClippingRankSum'], info_right['ClippingRankSum']),
        DP=mappend(operator.add, info_left['DP'], info_right['DP']),
        MQ=mappend(ext, info_left['MQ'], info_right['MQ']),
        MQRankSum=mappend(ext, info_left['MQRankSum'], info_right['MQRankSum']),
        QUALapprox=mappend(operator.add, info_left['QUALapprox'], info_right['QUALapprox']),
        MQ_DP=mappend(operator.add, info_left['MQ_DP'], info_right['MQ_DP']),
        RAW_MQ=mappend(operator.add, info_left['RAW_MQ'], info_right['RAW_MQ']),
        SB=mappend(operator.add, info_left['SB'], info_right['SB']),
        ReadPosRankSum=mappend(ext, info_left['ReadPosRankSum'], info_right['ReadPosRankSum']),
        VarDP=mappend(operator.add, info_left['VarDP'], info_right['VarDP']),
    )


def concatenate_entries(left, len_left, right, len_right) -> ArrayExpression:
    def empty_array(typ, length) -> ArrayExpression:
        return hl.range(0, length).map(lambda _: hl.null(typ))
    larr = hl.or_else(left, empty_array(left.dtype.element_type, len_left))
    rarr = hl.or_else(right, empty_array(right.dtype.element_type, len_right))
    return larr.extend(rarr)


def merge_alleles(left, right) -> ArrayExpression:
    tmp = left.filter(lambda e: e != '<NON-REF>')
    return tmp.extend(right.filter(lambda e: ~tmp.contains(e)))


def shuffle_pl(pl) -> ArrayExpression:
    return hl.cond(pl.length() == 10,
                   pl[:1].append(pl[3]).append(pl[5]).append(pl[1]).append(pl[4]).append(pl[2])
                         .append(pl[6]).append(pl[8]).append(pl[7]).append(pl[9]),
                   pl)


def renumber_gt(format_field, old, new) -> StructExpression:
    """renumbers GTs, for example suppose that an entry has GT=2/4, corresponding to the,
       3rd and 5th elements of old, but suppose that the 3rd element of old is the 8th element
       of new, and the 5th element of old is the 2nd element of new, then this function returns
       a format struct with a GT of 2/8 and will rearrange the PL array to account for switching
       up the order of the alleles in the GT"""
    def help1(call, old, new):
        al_0 = old[call[0]]
        new_ind_0 = position(new, lambda e: e == al_0)
        al_1 = old[call[1]]
        new_ind_1 = position(new, lambda e: e == al_1)
        return hl.call(new_ind_0, new_ind_1, phased=False), new_ind_1 < new_ind_0

    def help2(fld, old, new):
        ncall, flipped = help1(fld['GT'], old, new)
        fld = fld.annotate(GT=ncall)
        return hl.cond(flipped, fld.annotate(PL=shuffle_pl(fld['PL'])), fld)
    return hl.cond(format_field['GT'] == REF_CALL, format_field, help2(format_field, old, new))


# NOTE: separated out for taking measurements
def do_join(mt_left, mt_right) -> Table:
    # pylint: disable=protected-access
    tab_left = mt_left._localize_entries('__left')
    tab_right = mt_right._localize_entries('__right')
    return tab_left.join(tab_right, how='outer')


# NOTE: separated out for taking measurements
def transform_and_combine(mt_left, mt_right) -> Table:
    """Localizes two vcf-ish matrixtables, joins them, and combines their row fields
    does not unlocalize"""
    # pylint: disable=protected-access
    left_col_len = mt_left.cols().count()
    right_col_len = mt_right.cols().count()
    joined = do_join(mt_left, mt_right)
    filters = joined.transmute(
        filters=mappend(lambda s1, s2: s1.union(s2), joined.filters, joined.filters_1),
        rsid=hl.or_else(joined.rsid, joined.rsid_1))
    alleles = filters.transmute(
        alleles_right=filters.alleles_1,
        alleles=mappend(merge_alleles, filters.alleles, filters.alleles_1))
    renumbered = alleles.annotate(
        __right=alleles.__right.map(lambda fld: renumber_gt(fld, alleles.alleles_right, alleles.alleles))
        ).drop('alleles_right')
    entries = renumbered.transmute(
        __entries=concatenate_entries(
            renumbered.__left, left_col_len,
            renumbered.__right, right_col_len),
        info=combine_infos(renumbered.info, renumbered.info_1))
    return entries


def combine_two_vcfs(mt_left, mt_right) -> MatrixTable:
    """merges two vcfs, fixing up their fields"""
    # pylint: disable=protected-access
    combined_cols = mt_left.cols().union(mt_right.cols())
    entries = transform_and_combine(mt_left, mt_right)
    return entries._unlocalize_entries(combined_cols, '__entries')


def combine_tables(left, left_col_len, right, right_col_len) -> Table:
    """combines two already localized tables"""
    # pylint: disable=protected-access
    joined = left.join(right, how='outer')
    filters = joined.transmute(
        filters=mappend(lambda s1, s2: s1.union(s2), joined.filters, joined.filters_1),
        rsid=hl.or_else(joined.rsid, joined.rsid_1))
    alleles = filters.transmute(
        alleles_right=filters.alleles_1,
        alleles=mappend(merge_alleles, filters.alleles, filters.alleles_1))
    renumbered = alleles.annotate(
        __entries_1=alleles.__entries_1.map(lambda fld: renumber_gt(fld, alleles.alleles_right, alleles.alleles))
        ).drop('alleles_right')
    entries = renumbered.transmute(
        __entries=concatenate_entries(
            renumbered.__entries, left_col_len,
            renumbered.__entries_1, right_col_len),
        info=combine_infos(renumbered.info, renumbered.info_1))
    return entries, left_col_len + right_col_len


def combine_vcfs(*mts):
    """merges lists of vcf data returned with read_and_transform_one, or matrix tables in equivalent
    """
    # pylint: disable=protected-access
    def localize(mt):
        return mt._localize_entries('__entries')

    def combine_all(mts):
        """ does the combining """
        combined, count = None, None
        for mt, mtc in mts:
            if combined is None and count is None:
                combined, count = mt, mtc
                continue
            combined, count = combine_tables(combined, count, mt, mtc)
        return combined

    cols = None
    for mt in mts:
        if cols is None:
            cols = mt.cols()
        else:
            cols = cols.union(mt.cols())
    mts_lens = [(localize(mt), mt.cols().count()) for mt in mts]
    combined = combine_all(mts_lens)
    return combined._unlocalize_entries(cols, '__entries')


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
#       BaseQRankSum: median,
#       ClippingRankSum: median,
#       DP: sum
#       ExcessHet: median, # NOTE : this can also be dropped
#       MQ: median,
#       MQ_DP: sum,
#       MQ0: median,
#       MQRankSum: median,
#       QUALApprox: sum,
#       RAW_MQ: sum
#       ReadPosRankSum: median,
#       SB: elementwise sum, # NOTE: after being moved from FORMAT
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
