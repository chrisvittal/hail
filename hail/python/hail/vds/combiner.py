from typing import List

import hail as hl
from hail import MatrixTable, Table
from hail.ir import Apply, TableMapRows, TopLevelReference
from hail.typecheck import oneof, sequenceof, typecheck
from .variant_dataset import VariantDataset
from hail.experimental.vcf_combiner.vcf_combiner import combine_gvcfs, localize, parse_as_fields, unlocalize

_transform_variant_function_map = {}
_transform_reference_fuction_map = {}


def make_variants_table(mt, info_to_keep=[]) -> Table:
    mt = mt.filter(hl.is_missing(mt.info.END))

    if mt.row.dtype not in _transform_variant_function_map:
        def get_lgt(e, n_alleles, has_non_ref, row):
            index = e.GT.unphased_diploid_gt_index()
            n_no_nonref = n_alleles - hl.int(has_non_ref)
            triangle_without_nonref = hl.triangle(n_no_nonref)
            return (hl.case()
                    .when(e.GT.is_haploid(),
                          hl.or_missing(e.GT[0] < n_no_nonref, e.GT))
                    .when(index < triangle_without_nonref, e.GT)
                    .when(index < hl.triangle(n_alleles), hl.missing('call'))
                    .or_error('invalid GT ' + hl.str(e.GT) + ' at site ' + hl.str(row.locus)))

        def make_entry_struct(e, alleles_len, has_non_ref, row):
            handled_fields = dict()
            handled_names = {'LA', 'gvcf_info',
                             'LAD', 'AD',
                             'LGT', 'GT',
                             'LPL', 'PL',
                             'LPGT', 'PGT'}

            if 'GT' not in e:
                raise hl.utils.FatalError("the Hail GVCF combiner expects GVCFs to have a 'GT' field in FORMAT.")

            handled_fields['LA'] = hl.range(0, alleles_len - hl.if_else(has_non_ref, 1, 0))
            handled_fields['LGT'] = get_lgt(e, alleles_len, has_non_ref, row)
            if 'AD' in e:
                handled_fields['LAD'] = hl.if_else(has_non_ref, e.AD[:-1], e.AD)
            if 'PGT' in e:
                handled_fields['LPGT'] = e.PGT
            if 'PL' in e:
                handled_fields['LPL'] = hl.if_else(has_non_ref,
                                                   hl.if_else(alleles_len > 2,
                                                              e.PL[:-alleles_len],
                                                              hl.missing(e.PL.dtype)),
                                                   hl.if_else(alleles_len > 1,
                                                              e.PL,
                                                              hl.missing(e.PL.dtype)))
                handled_fields['RGQ'] = hl.if_else(
                    has_non_ref,
                    hl.if_else(e.GT.is_haploid(),
                               e.PL[alleles_len - 1],
                               e.PL[hl.call(0, alleles_len - 1).unphased_diploid_gt_index()]),
                    hl.missing(e.PL.dtype.element_type))

            handled_fields['gvcf_info'] = (hl.case()
                                           .when(hl.is_missing(row.info.END),
                                                 hl.struct(**(
                                                     parse_as_fields(
                                                         row.info.select(*info_to_keep),
                                                         has_non_ref)
                                                 )))
                                           .or_missing())

            pass_through_fields = {k: v for k, v in e.items() if k not in handled_names}
            return hl.struct(**handled_fields, **pass_through_fields)

        f = hl.experimental.define_function(
            lambda row: hl.rbind(
                hl.len(row.alleles), '<NON_REF>' == row.alleles[-1],
                lambda alleles_len, has_non_ref: hl.struct(
                    locus=row.locus,
                    alleles=hl.if_else(has_non_ref, row.alleles[:-1], row.alleles),
                    rsid=row.rsid,
                    __entries=row.__entries.map(
                        lambda e: make_entry_struct(e, alleles_len, has_non_ref, row)))),
            mt.row.dtype)
        _transform_variant_function_map[mt.row.dtype] = f
    transform_row = _transform_variant_function_map[mt.row.dtype]
    return Table(TableMapRows(mt._tir, Apply(transform_row._name, transform_row._ret_type, TopLevelReference('row'))))


def make_reference_table(mt) -> Table:
    mt = mt.filter(hl.is_defined(mt.info.END))

    def make_entry_struct(e, row):
        reference_fields = {k: v for k, v in e.items() if k in ('DP', 'GQ', 'MIN_DP')}
        return (hl.case()
                  .when(e.GT.is_hom_ref(), hl.struct(END=row.info.END, **reference_fields))
                  .or_error('found END with non reference-genotype at' + hl.str(row.locus)))

    if mt.row.dtype not in _transform_reference_fuction_map:
        f = hl.experimental.define_function(
            lambda row: hl.struct(
                locus=row.locus,
                ref_allele=row.alleles[0][0],
                __entries=row.__entries.map(
                    lambda e: make_entry_struct(e, row))),
            mt.row.dtype)
        _transform_reference_fuction_map[mt.row.dtype] = f

    transform_row = _transform_reference_fuction_map[mt.row.dtype]
    return Table(TableMapRows(mt._tir, Apply(transform_row._name, transform_row._ret_type, TopLevelReference('row'))))


@typecheck(mt=oneof(Table, MatrixTable), info_to_keep=sequenceof(str))
def transform_gvcf(mt, info_to_keep=[]) -> VariantDataset:
    """Transforms a gvcf into a sparse matrix table

    The input to this should be some result of either :func:`.import_vcf` or
    :func:`.import_gvcfs` with ``array_elements_required=False``.

    There is an assumption that this function will be called on a matrix table
    with one column (or a localized table version of the same).

    Parameters
    ----------
    mt : :obj:`Union[Table, MatrixTable]`
        The gvcf being transformed, if it is a table, then it must be a localized matrix table with
        the entries array named ``__entries``
    info_to_keep : :obj:`List[str]`
        Any ``INFO`` fields in the gvcf that are to be kept and put in the ``gvcf_info`` entry
        field. By default, all ``INFO`` fields except ``END`` and ``DP`` are kept.

    Returns
    -------
    :obj:`.Table`
        A localized matrix table that can be used as part of the input to :func:`.combine_gvcfs`

    Notes
    -----
    This function will parse the following allele specific annotations from
    pipe delimited strings into proper values. ::

        AS_QUALapprox
        AS_RAW_MQ
        AS_RAW_MQRankSum
        AS_RAW_ReadPosRankSum
        AS_SB_TABLE
        AS_VarDP

    """
    if not info_to_keep:
        info_to_keep = [name for name in mt.info if name not in ['END', 'DP']]
    mt = localize(mt)
    ref_mt = make_reference_table(mt)
    var_mt = make_variants_table(mt, info_to_keep)
    return VariantDataset(unlocalize(ref_mt), unlocalize(var_mt))


_merge_function_map = {}


def combine_r(ts):
    if (ts.row.dtype, ts.globals.dtype) not in _merge_function_map:
        f = hl.experimental.define_function(
            lambda row, gbl:
            hl.struct(
                locus=row.locus,
                ref_allele=hl.find(hl.is_defined, row.data.map(lambda d: d.ref_allele)),
                __entries=hl.range(0, hl.len(row.data)).flatmap(
                    lambda i:
                    hl.if_else(hl.is_missing(row.data[i]),
                               hl.range(0, hl.len(gbl.g[i].__cols))
                               .map(lambda _: hl.missing(row.data[i].__entries.dtype.element_type)),
                               row.data[i].__entries))))
        _merge_function_map[(ts.row.dtype, ts.globals.dtype)] = f
    merge_function = _merge_function_map[(ts.row.dtype, ts.globals.dtype)]
    ts = Table(TableMapRows(ts._tir, Apply(merge_function._name,
                                           merge_function._ret_type,
                                           TopLevelReference('row'),
                                           TopLevelReference('global'))))
    return ts.transmute_globals(__cols=hl.flatten(ts.g.map(lambda g: g.__cols)))


def combine_references(mts: List[MatrixTable]) -> MatrixTable:
    ts = hl.Table.multi_way_zip_join([localize(mt) for mt in mts], 'data', 'g')
    combined = combine_r(ts)
    return unlocalize(combined)


def combine_varant_datasets(vdss: List[VariantDataset]) -> VariantDataset:
    reference = combine_references([vds.reference_data for vds in vdss])
    variants = combine_gvcfs([vds.variant_data for vds in vdss])
    return VariantDataset(reference, variants)