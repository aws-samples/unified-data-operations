# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os, sys, zipfile, itertools
from typing import List, Dict
from pip._vendor import pkg_resources


def install_pip_package(packages: list):
    from pip._internal.commands import create_command
    install = create_command('install', isolated=False)
    install.main(packages)


def ziplib(dist_path, package_name) -> str:
    libpath = os.path.dirname(os.path.join(dist_path, package_name))
    zippath = f'{package_name}.zip'
    zf = zipfile.PyZipFile(zippath, mode='w')
    try:
        zf.debug = 3
        zf.writepy(libpath)
        return zippath
    finally:
        zf.close()


def install_dependencies(product_path: str) -> Dict:
    def collect_packages() -> set:
        ws = pkg_resources.WorkingSet(pkg_resources.working_set.entries)
        eks = ws.entry_keys
        return set(itertools.chain(*[eks.get(k) for k in eks.keys()]))

    def find_path_for_package(package_name):
        ws = pkg_resources.WorkingSet(pkg_resources.working_set.entries)
        eks = ws.entry_keys
        return next(iter([path for path in eks.keys() if package_name in eks.get(path)]), None)

        #todo: review and remove the one below

        # def collect_deps(package_name: str):
        #     def merge_reqs(package: pkg_resources.DistInfoDistribution):
        #         return_set = set({package.project_name})
        #         required_deps: List[pkg_resources.Requirement] = p.requires()
        #         required_pnames = [r.project_name for r in required_deps]
        #         for rpack in required_pnames:
        #             return_set.update(merge_reqs(rpack))
        #         return return_set
        # ws = pkg_resources.WorkingSet(pkg_resources.working_set.entries)
        # package: pkg_resources.DistInfoDistribution = ws.by_key[package_name]
        # return merge_reqs(package)

    requirements = os.path.join(product_path, 'requirements.txt')
    if os.path.isfile(requirements):
        before = collect_packages()
        with open(requirements) as f:
            lines = [line.rstrip('\n') for line in f]
            install_pip_package(lines)
        after = collect_packages()
        delta_packages = after - before
        return_packs = dict()
        for delta_pack in delta_packages:
            return_packs[delta_pack] = find_path_for_package(delta_pack)
        return return_packs
