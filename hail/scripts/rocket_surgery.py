#!/usr/bin/env python3
import asyncio
import functools
import gzip
import io
import json
import os
import urllib.parse

from hail.vds import VariantDataset
from hailtop import uvloopx
from hailtop.aiotools import Copier, Transfer
from hailtop.aiotools.copy import GrowingSempahore
from hailtop.aiotools.fs import IsABucketError
from hailtop.aiotools.router_fs import RouterAsyncFS
from hailtop.utils import bounded_gather2, retry_transient_errors
from hailtop.utils.rich_progress_bar import CopyToolProgressBar

MD = 'metadata.json.gz'
VDS_PATHS = [
    'REDACTED'
]
OUTPUT_PATH = 'REDACTED'
MAX_TRANSFERS = 75
INITIAL_TRANSFERS = 10


async def main():
    async with RouterAsyncFS() as fs:
        ref_paths = [VariantDataset._reference_path(path) for path in VDS_PATHS]
        ref_out = VariantDataset._reference_path(OUTPUT_PATH)
        print('Merging reference data')
        await merge(fs, ref_paths, ref_out)
        var_paths = [VariantDataset._variants_path(path) for path in VDS_PATHS]
        var_out = VariantDataset._variants_path(OUTPUT_PATH)
        print('Merging variant data')
        await merge(fs, var_paths, var_out)


async def merge(fs, ins, out):
    # copy cols, globals, other common files from first item
    names = [
        'README.txt',
        '_SUCCESS',
        'cols/README.txt',
        'cols/_SUCCESS',
        'cols/metadata.json.gz',
        'cols/rows/metadata.json.gz',
        'cols/rows/parts/part-0',
        'entries/README.txt',
        'entries/_SUCCESS',
        'globals/README.txt',
        'globals/_SUCCESS',
        'globals/globals/metadata.json.gz',
        'globals/globals/parts/part-0',
        'globals/metadata.json.gz',
        'globals/rows/metadata.json.gz',
        'globals/rows/parts/part-0',
        'rows/README.txt',
        'rows/_SUCCESS',
    ]

    common_transfers = [
        Transfer(os.path.join(ins[0], name), os.path.join(out, name), treat_dest_as=Transfer.DEST_IS_TARGET)
        for name in names
    ]

    new_mt_spec = merge_rel_specs(*await asyncio.gather(*(read_md(fs, os.path.join(root, MD)) for root in ins)))
    new_rows_spec = merge_rel_specs(
        *await asyncio.gather(*(read_md(fs, os.path.join(root, 'rows', MD)) for root in ins))
    )
    new_entries_spec = merge_rel_specs(
        *await asyncio.gather(*(read_md(fs, os.path.join(root, 'entries', MD)) for root in ins))
    )

    rows_rvd_specs = await asyncio.gather(
        *(rvd_spec_and_part_paths(fs, os.path.join(root, 'rows', 'rows')) for root in ins)
    )
    entries_rvd_specs = await asyncio.gather(
        *(rvd_spec_and_part_paths(fs, os.path.join(root, 'entries', 'rows')) for root in ins)
    )
    index_paths = [
        os.path.join(inp, 'index', part) + '.idx'
        for (spec, _), inp in zip(rows_rvd_specs, ins)
        for part in spec['_partFiles']
    ]
    assert len(index_paths) == len(new_mt_spec['components']['partition_counts']['counts'])
    assert len(index_paths) == len(new_rows_spec['components']['partition_counts']['counts'])
    assert len(index_paths) == len(new_entries_spec['components']['partition_counts']['counts'])

    n_new_parts = len(index_paths)
    padding = len(str(n_new_parts))
    new_parts = ['part-' + str(i).rjust(padding, '0') for i in range(n_new_parts)]

    index_root = os.path.join(out, 'index')
    index_transfers = [
        Transfer(os.path.join(idx_path, MD), os.path.join(index_root, part + '.idx', MD))
        for idx_path, part in zip(index_paths, new_parts)
    ] + [
        Transfer(os.path.join(idx_path, 'index'), os.path.join(index_root, part + '.idx', 'index'))
        for idx_path, part in zip(index_paths, new_parts)
    ]

    rows_part_root = os.path.join(out, 'rows', 'rows', 'parts')
    rows_transfers = [
        Transfer(old_part, os.path.join(rows_part_root, new_part))
        for old_part, new_part in zip((old_part for _, parts in rows_rvd_specs for old_part in parts), new_parts)
    ]

    entries_part_root = os.path.join(out, 'entries', 'rows', 'parts')
    entries_transfers = [
        Transfer(old_part, os.path.join(entries_part_root, new_part))
        for old_part, new_part in zip((old_part for _, parts in entries_rvd_specs for old_part in parts), new_parts)
    ]

    print('Checking for output file existence')
    common_exists = await asyncio.gather(*(fs.exists(t.dest) for t in common_transfers))
    common_transfers = [] if all(common_exists) else common_transfers

    try:
        n_index_files = len([it async for it in await fs.listfiles(index_root)])
        index_transfers = index_transfers if n_new_parts != n_index_files else []
    except FileNotFoundError:
        pass

    try:
        n_rows_parts = len([it async for it in await fs.listfiles(rows_part_root)])
        rows_transfers = rows_transfers if n_new_parts != n_rows_parts else []
    except FileNotFoundError:
        pass

    try:
        n_entries_parts = len([it async for it in await fs.listfiles(entries_part_root)])
        entries_transfers = entries_transfers if n_new_parts != n_entries_parts else []
    except FileNotFoundError:
        pass

    new_rows_rvd_spec = merge_rvd_specs(new_parts, *(spec for spec, _ in rows_rvd_specs))
    new_entries_rvd_spec = merge_rvd_specs(new_parts, *(spec for spec, _ in entries_rvd_specs))

    args = (
        (fs, os.path.join(out, MD), new_mt_spec),
        (fs, os.path.join(out, 'rows', MD), new_rows_spec),
        (fs, os.path.join(out, 'entries', MD), new_entries_spec),
        (fs, os.path.join(out, 'rows', 'rows', MD), new_rows_rvd_spec),
        (fs, os.path.join(out, 'entries', 'rows', MD), new_entries_rvd_spec),
    )
    md_exists = await asyncio.gather(*(fs.exists(path) for _, path, _ in args))
    transfers = common_transfers + index_transfers + rows_transfers + entries_transfers

    if all(md_exists) and not transfers:
        print('Nothing to do!')
        return
    if not all(md_exists):
        print('Writing new metadata')
        await asyncio.gather(*(write_md(fs, path, md) for fs, path, md in args))
    else:
        print('All new metadata exists')

    print('Starting copy')
    with CopyToolProgressBar() as progress:
        parallelism_tid = progress.add_task(
            description='parallelism',
            completed=INITIAL_TRANSFERS,
            total=MAX_TRANSFERS,
        )
        async with GrowingSempahore(
            INITIAL_TRANSFERS,
            MAX_TRANSFERS,
            progress_and_tid=(progress, parallelism_tid),
        ) as sema:
            file_tid = progress.add_task(description='files', total=len(transfers))
            requests_tid = progress.add_task(description='requests', total=None)

            # if we get here, gfs should be initialized
            gfs = fs._google_fs
            assert gfs is not None
            await bounded_gather2(
                sema,
                *[functools.partial(rewrite, gfs, t.src, t.dest, progress, file_tid, requests_tid) for t in transfers],
                cancel_on_error=True,
            )


async def read_md(fs, path):
    md_bytes = await fs.read(path)
    with gzip.open(io.BytesIO(md_bytes)) as stream:
        return json.load(stream)


def merge_rel_specs(md, *mds) -> dict:
    for it in mds:
        md['components']['partition_counts']['counts'] += it['components']['partition_counts']['counts']
        if md['name'] == 'TableSpec':
            md['components']['properties']['properties']['distinctlyKeyed'] &= it['components']['properties'][
                'properties'
            ]['distinctlyKeyed']
    return md


async def rvd_spec_and_part_paths(fs, base):
    md = await read_md(fs, os.path.join(base, MD))
    parts = [os.path.join(base, 'parts', file) for file in md['_partFiles']]
    return md, parts


def merge_rvd_specs(new_parts, spec, *specs):
    for it in specs:
        spec['_jRangeBounds'] += it['_jRangeBounds']
    assert len(spec['_jRangeBounds']) == len(new_parts)
    spec['_partFiles'] = new_parts
    return spec


async def write_md(fs, path, md):
    jstr = gzip.compress(json.dumps(md).encode())
    try:
        await fs.write(path, jstr)
    except FileNotFoundError:
        base, _ = os.path.split(path)
        await fs.makedirs(base, exist_ok=True)
        await fs.write(path, jstr)


import rich.progress
from typing import Optional
from hailtop.aiocloud.aiogoogle import GoogleStorageAsyncFS


async def rewrite(
    gfs: GoogleStorageAsyncFS,
    src: str,
    dst: str,
    progress: Optional[rich.progress.Progress] = None,
    file_tid: Optional[rich.progress.TaskID] = None,
    requests_tid: Optional[rich.progress.TaskID] = None,
):
    assert (progress is None) == (file_tid is None) == (requests_tid is None)
    src_bkt, src_name = gfs.get_bucket_and_name(src)
    dst_bkt, dst_name = gfs.get_bucket_and_name(dst)
    if not src_name:
        raise IsABucketError(src)
    if not dst_name:
        raise IsABucketError(dst)
    client = gfs._storage_client
    path = (
        f'/b/{src_bkt}/o/{urllib.parse.quote(src_name, safe="")}/rewriteTo'
        f'/b/{dst_bkt}/o/{urllib.parse.quote(dst_name, safe="")}'
    )
    kwargs = {'json': '', 'params': {}}
    client._update_params_with_user_project(kwargs, src_bkt)
    response = await retry_transient_errors(client.post, path, **kwargs)
    if progress is not None:
        progress.update(requests_tid, advance=1)
    while not response['done']:
        kwargs['params']['rewriteToken'] = response['rewriteToken']
        response = await retry_transient_errors(client.post, path, **kwargs)
        if progress is not None:
            progress.update(requests_tid, advance=1)
    if progress is not None:
        progress.update(file_tid, advance=1)


if __name__ == "__main__":
    uvloopx.install()
    asyncio.run(main())
