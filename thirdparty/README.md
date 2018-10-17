# External dependencies

These are a collection of scripts used to build third party native dependencies of hail.
They are styled after linux package manager scripts. To build a dependency, run

```sh
    $ ./hailpkg.sh <package.hailbuild>
```

This will output a tar in the working directory containing the built artifacts. The intention
is for these to be published and used in creating the distribution.
