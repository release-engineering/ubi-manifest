from pubtools.pulplib import Criteria, YumRepository
from ubiconfig import UbiConfig

from ubi_manifest.worker.models import PackageToExclude
from ubi_manifest.worker.pulp_queries import search_modulemds
from ubi_manifest.worker.utils import is_blacklisted


def filter_whitelist(
    ubi_config: UbiConfig, blacklist: list[PackageToExclude]
) -> tuple[set[str], set[str]]:
    """
    Produce whitelist and debuginfo_whitelist, filtering out src and blacklisted packages.
    """
    whitelist = set()
    debuginfo_whitelist = set()

    for pkg in ubi_config.packages.whitelist:
        if pkg.arch == "src":
            continue
        if is_blacklisted(pkg, blacklist):
            continue
        if (
            pkg.name.endswith("debuginfo")
            or pkg.name.endswith("debugsource")
            or pkg.name.endswith("debuginfo-common")
        ):
            debuginfo_whitelist.add(pkg.name)
        else:
            whitelist.add(pkg.name)

    return whitelist, debuginfo_whitelist


def get_pkgs_from_all_modules(repos: list[YumRepository]) -> set[str]:
    """
    Search for modulemds in all input repos and extract rpm filenames.
    """

    def extract_modular_filenames() -> set[str]:
        filenames = set()
        for module in modules:  # type: ignore [attr-defined]
            filenames |= set(module.artifacts_filenames)

        return filenames

    modules = search_modulemds([Criteria.true()], repos)
    return extract_modular_filenames()
