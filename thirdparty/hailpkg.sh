#!/bin/sh
set -e

if [ -z ${BUILDDIR+x} ]; then
    BUILDDIR=$PWD
fi
PKGDIR=$(realpath ${PKGDIR:-$PWD})

if [ -z $1 ]; then
    echo usage: $0 '<build_script>' >&2
    exit 1
fi

package() {
    tar cf - dist | xz -v -9 > $PKGDIR/$pkg_name-$pkg_version.pkg.tar.xz
}

. $(realpath $1)

if [ ! -d $BUILDDIR ]; then mkdir $BUILDDIR; fi
if [ ! -d $PKGDIR ]; then mkdir $PKGDIR; fi

pkg_dir=$(realpath $BUILDDIR/$pkg_name-$pkg_version)
src_dir=$pkg_dir/src
dist_dir=$pkg_dir/dist
mkdir -p $src_dir $dist_dir

cd $pkg_dir
if [ -z ${HAIL_PKG_NOFETCH+x} ]; then
    printf "\tHailbuild: fetch\n"
    fetch
fi
cd $pkg_dir
if [ -z ${HAIL_PKG_NOPREP+x} ]; then
    printf "\tHailbuild: prepare\n"
    prepare
fi
cd $pkg_dir
if [ -z ${HAIL_PKG_NOBUILD+x} ]; then
    printf "\tHailbuild: compile\n"
    build
fi
cd $pkg_dir
if [ -z ${HAIL_PKG_NOINSTALL+x} ]; then
    printf "\tHailbuild: install\n"
    install
fi
cd $pkg_dir
if [ -z ${HAIL_PKG_NOPKG+x} ]; then
    printf "\tHailbuild: package\n"
    package
fi
printf "\tHailbuild: done\n"
