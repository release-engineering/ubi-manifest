import logging
from typing import Optional

from pubtools.pulplib import (
    Client,
    Criteria,
    YumRepository,
)
from pydantic import BaseModel, ConfigDict

from ubi_manifest.worker.common import filter_whitelist, get_pkgs_from_all_modules
from ubi_manifest.worker.models import PackageToExclude, UbiUnit
from ubi_manifest.worker.pulp_queries import search_rpms
from ubi_manifest.worker.tasks.depsolver.rpm_depsolver import BATCH_SIZE_RPM
from ubi_manifest.worker.ubi_config import (
    UbiConfigLoader,
    get_content_config,
)
from ubi_manifest.worker.utils import (
    RELATION_CMP_MAP,
    get_n_latest_from_content,
    is_blacklisted,
    parse_blacklist_config,
    split_filename,
)

_LOG = logging.getLogger(__name__)


class RepoContent(BaseModel):
    """
    A model for storing pre-processed, auditing ready repo content.

    Attributes:
        nonmodular_rpm_units (set[UbiUnit]): All non-modular RPM units.
        modulemd_units (Optional[set[UbiUnit]]): Optional set of ModuleMD units.
        modulemd_defaults (Optional[set[UbiUnit]]): Optional set of ModuleMD defaults.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    nonmodular_rpm_units: set[UbiUnit] = set()  # all non modular rpm units
    modulemd_units: Optional[set[UbiUnit]] = None
    modulemd_defaults: Optional[set[UbiUnit]] = None


class NonModularAuditor:
    """
    A class to audit non-modular RPM units against whitelist and blacklist rules.

    Attributes:
        out_repo_id (str): The ID of the output repository.
        whitelist (set[str]): A set of whitelisted package names.
        blacklists (dict[str, set[PackageToExclude]]): Dictionary containing the srpm and rpm packages to exclude.
        arranged_in_units (dict[tuple[str, str], UbiUnit]): Input units to be audited.
        arranged_out_units (dict[tuple[str, str], UbiUnit]): Output units to be audited.
        src_units (list[UbiUnit]): Units in the output source repo.
    """

    def __init__(
        self,
        out_repo_id: Optional[str] = None,
        whitelist: Optional[set[str]] = None,
        blacklists: Optional[dict[str, set[PackageToExclude]]] = None,
        arranged_in_units: Optional[dict[tuple[str, str], UbiUnit]] = None,
        arranged_out_units: Optional[dict[tuple[str, str], UbiUnit]] = None,
        src_units: Optional[list[UbiUnit]] = None,
    ) -> None:
        self.whitelist = whitelist
        self.blacklists = blacklists
        self.arranged_in_units = arranged_in_units
        self.arranged_out_units = arranged_out_units
        self.src_units = src_units
        self.out_repo_id = out_repo_id

    def validate_versions(self) -> None:
        """
        Validates the versions of input and output RPM units.

        Logs a warning if an output unit is outdated compared to an input unit.
        """

        def log_warning(
            warn_tuple: tuple[str, tuple[str, str, str], tuple[str, str, str]],
            arch: str,
        ) -> None:
            _LOG.warning(
                "[%s] UBI rpm of %s '%s' is outdated (current: %s, latest: %s)",
                self.out_repo_id,
                arch,
                *warn_tuple,
            )

        for name_arch, out_unit in self.arranged_out_units.items():  # type: ignore
            if name_arch not in self.arranged_in_units:  # type: ignore
                continue
            out_evr = (out_unit.epoch, out_unit.version, out_unit.release)
            in_unit = self.arranged_in_units[name_arch]  # type: ignore
            in_evr = (in_unit.epoch, in_unit.version, in_unit.release)

            if RELATION_CMP_MAP["LT"](
                out_evr,
                in_evr,
            ):
                log_warning((out_unit.name, out_evr, in_evr), name_arch[1])

    def verify_blacklist(self, is_src_repo: bool = False) -> None:
        """
        Verifies that no blacklisted packages are present in the output units.

        Logs a warning if any blacklisted packages are found.
        """
        if is_src_repo:
            blacklisted_pkgs = {
                u.name
                for u in self.src_units  # type: ignore
                if is_blacklisted(u, list(self.blacklists["srpm_packages_to_exclude"]))  # type: ignore
            }

        else:
            blacklisted_pkgs = {
                u.name
                for u in self.arranged_out_units.values()  # type: ignore
                if is_blacklisted(u, list(self.blacklists["packages_to_exclude"]))  # type: ignore
            }

        if blacklisted_pkgs:
            _LOG.warning(
                "[%s] blacklisted content found in output repository;\n\t%s",
                self.out_repo_id,
                "\n\t".join(sorted(blacklisted_pkgs)),
            )

    def verify_whitelist(self) -> None:
        """
        Verifies that whitelisted packages are present in the input and output units.

        Logs information and warnings based on the presence of whitelisted packages.
        """
        for whitelisted_pkg_name in self.whitelist:  # type: ignore
            in_input_repos = any(
                unit.name == whitelisted_pkg_name
                for unit in self.arranged_in_units.values()  # type: ignore
            )
            in_output_repo = any(
                unit.name == whitelisted_pkg_name
                for unit in self.arranged_out_units.values()  # type: ignore
            )

            if in_input_repos and in_output_repo:
                continue

            if not in_input_repos and not in_output_repo:
                # ok
                _LOG.info(
                    "[%s] Whitelisted package '%s' not found in any input or output repositories.",
                    self.out_repo_id,
                    whitelisted_pkg_name,
                )
            elif not in_input_repos and in_output_repo:
                # whitelisted package should not be present only in one of the two
                _LOG.warning(
                    "[%s] Whitelisted package '%s' found in out repo but not in any input repos!",
                    self.out_repo_id,
                    whitelisted_pkg_name,
                )
            elif in_input_repos and not in_output_repo:
                # whitelisted package should not be present only in one of the two
                _LOG.warning(
                    "[%s] Whitelisted package '%s' found in input repos but not in output repo!",
                    self.out_repo_id,
                    whitelisted_pkg_name,
                )

    def verify_sources(self) -> None:
        """
        Verifies that all output RPMs have matching SRPMs in the source repo.

        Logs warnings if any packages are missing from the source repo.
        """
        for out_unit in self.arranged_out_units.values():  # type: ignore
            is_rpm_blacklisted = is_blacklisted(
                out_unit, list(self.blacklists["packages_to_exclude"])  # type: ignore
            )
            if is_rpm_blacklisted:
                continue

            is_srpm_blacklisted = any(
                blacklisted_package.name == split_filename(out_unit.sourcerpm)[0]
                for blacklisted_package in self.blacklists["srpm_packages_to_exclude"]  # type: ignore
            )
            if is_srpm_blacklisted:
                continue

            in_src_repo = any(
                src_unit.filename == out_unit.sourcerpm
                for src_unit in self.src_units  # type: ignore
            )
            if in_src_repo:
                continue

            _LOG.warning(
                "SRPM '%s' for RPM '%s' is missing in the source repository",
                out_unit.sourcerpm,
                out_unit.name,
            )


class ContentProcessor:
    """
    A class to process and audit repository content.

    Attributes:
        client (Client): The client used to interact with the repository.
        out_repo_bundle (dict[str, YumRepository]): A bundle of bin, debug and source out repos.
        in_repos_bundle (dict[str, list[YumRepository]]):
            Input repos aggregated by bin, debug or source type.
        config_loader (UbiConfigLoader): The configuration loader.
        all_modular_filenames (set[str]): A set of all modular filenames.
        nonmodular_auditor (NonModularAuditor): Auditor for non-modular RPMs.
        out_repo_content (RepoContent): Content of the output repository.
        in_repos_content (RepoContent): Content of the input repositories.
    """

    def __init__(
        self,
        client: Client,
        out_repo_bundle: dict[str, YumRepository],
        in_repos_bundle: dict[str, list[YumRepository]],
        config_loader: UbiConfigLoader,
    ) -> None:
        self.client = client
        self.out_repo_bundle = out_repo_bundle
        self.in_repos_bundle = in_repos_bundle
        self.config_loader = config_loader
        self.all_modular_filenames = self._get_all_modular_filenames()

        self.nonmodular_auditor = NonModularAuditor()
        self.out_repo_content = RepoContent()
        self.in_repos_content = RepoContent()
        self.criteria: list[Criteria] = []

    def _get_all_modular_filenames(self) -> set[str]:
        all_modular_filenames = get_pkgs_from_all_modules(
            self.in_repos_bundle["bin_repos"]
        )
        return all_modular_filenames

    def process_and_audit_bundle(self) -> None:
        """
        Processes a bundle of UBI repos (bin, debug, source) in sequence.
        """
        _LOG.info(
            "Auditing bundle of UBI repos [%s, %s, %s]...\n",
            self.out_repo_bundle["bin_repo"].id,
            self.out_repo_bundle["debug_repo"].id,
            self.out_repo_bundle["source_repo"].id,
        )
        self._fetch_src_repo_content()
        for repo_type in ["bin_repo", "debug_repo", "source_repo"]:
            out_repo = self.out_repo_bundle[repo_type]
            in_repos = self.in_repos_bundle.get(f"{repo_type}s", [])
            if repo_type == "bin_repo":
                _LOG.info(
                    "Processing and auditing UBI repo '%s' with modular content...",
                    out_repo.id,
                )
                _LOG.warning(
                    "Only auditing of non modular content has been implemented.\n"
                )
            else:
                _LOG.info("Processing and auditing UBI repo '%s'...\n", out_repo.id)
            self.nonmodular_auditor.out_repo_id = out_repo.id
            self._process_and_audit_type(out_repo, in_repos, repo_type)

    def _process_and_audit_type(
        self,
        out_repo: YumRepository,
        in_repos: list[YumRepository],
        repo_type: str,
    ) -> None:
        """
        Processes and audits the repository content.

        Fetches content from the output and input repositories, sets the
        whitelist and blacklist, and performs validation and checks.
        """
        self._set_whitelist_blacklist(out_repo, in_repos, repo_type)
        if repo_type != "source_repo":
            self._fetch_out_repo_content(out_repo)
            self._fetch_in_repos_contents(in_repos)
            self.nonmodular_auditor.validate_versions()
            self.nonmodular_auditor.verify_whitelist()
            self.nonmodular_auditor.verify_blacklist()
            self.nonmodular_auditor.verify_sources()
        else:
            self.nonmodular_auditor.verify_blacklist(is_src_repo=True)

    def _fetch_src_repo_content(self) -> None:
        """
        Fetches the content of the output src repository.

        Returns a list of the src repository UbiUnits
        """
        src_repo = self.out_repo_bundle["source_repo"]
        all_srpm_units: set[UbiUnit] = search_rpms(
            [Criteria.true()], [src_repo], BATCH_SIZE_RPM
        ).result()

        self.nonmodular_auditor.src_units = [
            unit
            for unit in all_srpm_units
            if unit.filename not in self.all_modular_filenames
        ]

    def _fetch_out_repo_content(self, out_repo: YumRepository) -> None:
        """
        Fetches the content of the output repository.

        Updates the nonmodular RPM units in the output repository content
        and the auditor.
        """
        all_rpm_units: set[UbiUnit] = search_rpms(
            [Criteria.true()], [out_repo], BATCH_SIZE_RPM
        ).result()
        non_modular_rpms = get_n_latest_from_content(
            all_rpm_units, modular_rpms=self.all_modular_filenames
        )
        self.out_repo_content.nonmodular_rpm_units = set(non_modular_rpms)
        # arranging units by name and architecture
        self.nonmodular_auditor.arranged_out_units = {}
        for unit in self.out_repo_content.nonmodular_rpm_units:
            self.nonmodular_auditor.arranged_out_units[(unit.name, unit.arch)] = unit

    def _fetch_in_repos_contents(self, in_repos: list[YumRepository]) -> None:
        """
        Fetches the contents of the input repositories.

        Updates the nonmodular RPM units in the input repositories content
        and the auditor.
        """
        rpm_units_criteria: list[Criteria] = []
        out_repo_pkgs = {
            unit.name for unit in self.out_repo_content.nonmodular_rpm_units
        }
        for pkg in self.nonmodular_auditor.whitelist | out_repo_pkgs:  # type: ignore
            rpm_units_criteria.append(Criteria.with_field("name", pkg))

        all_rpm_units: set[UbiUnit] = search_rpms(
            rpm_units_criteria, in_repos, BATCH_SIZE_RPM
        ).result()
        non_modular_rpms = get_n_latest_from_content(
            all_rpm_units, modular_rpms=self.all_modular_filenames
        )
        self.in_repos_content.nonmodular_rpm_units = set(non_modular_rpms)
        self.nonmodular_auditor.arranged_in_units = {}
        for unit in self.in_repos_content.nonmodular_rpm_units:
            self.nonmodular_auditor.arranged_in_units[(unit.name, unit.arch)] = unit

    def _set_whitelist_blacklist(
        self, out_repo: YumRepository, in_repos: list[YumRepository], repo_type: str
    ) -> None:
        """
        Sets the whitelist and blacklist based on the configuration of input repositories.

        Updates the auditor's whitelist and blacklist attributes.
        """
        current_whitelist = set()
        current_blacklists: dict[str, set[PackageToExclude]] = {
            "packages_to_exclude": set(),
            "srpm_packages_to_exclude": set(),
        }
        for in_repo in in_repos:
            config = get_content_config(
                self.config_loader,
                in_repo.content_set,
                out_repo.content_set,
                out_repo.ubi_config_version,
            )

            blacklist_result = parse_blacklist_config(config)
            for list_name, blacklist in blacklist_result.items():
                current_blacklists[list_name].update(blacklist)

            pkg_whitelist, debuginfo_whitelist = filter_whitelist(
                config, list(current_blacklists["packages_to_exclude"])
            )
            if repo_type == "debug_repo":
                current_whitelist.update(debuginfo_whitelist)
            elif repo_type == "bin_repo":
                current_whitelist.update(pkg_whitelist)

        self.nonmodular_auditor.whitelist = current_whitelist
        self.nonmodular_auditor.blacklists = current_blacklists
