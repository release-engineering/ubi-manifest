import logging
from typing import List, Optional, Set

from pubtools.pulplib import (
    Client,
    Criteria,
    RpmUnit,
    YumRepository,
)
from pydantic import BaseModel, ConfigDict

from ubi_manifest.worker.common import filter_whitelist
from ubi_manifest.worker.models import PackageToExclude, UbiUnit
from ubi_manifest.worker.pulp_queries import search_units
from ubi_manifest.worker.ubi_config import (
    UbiConfigLoader,
    get_content_config,
)
from ubi_manifest.worker.utils import (
    RELATION_CMP_MAP,
    get_n_latest_from_content,
    is_blacklisted,
    parse_blacklist_config,
)

_LOG = logging.getLogger(__name__)

RPM_FIELDS = ["name", "version", "release", "arch", "filename"]


class RepoContent(BaseModel):
    """
    A model for storing pre-processed, auditing ready repo content.

    Attributes:
        nonmodular_rpm_units (Set[UbiUnit]): All non-modular RPM units.
        modulemd_units (Optional[Set[UbiUnit]]): Optional set of ModuleMD units.
        modulemd_defaults (Optional[Set[UbiUnit]]): Optional set of ModuleMD defaults.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    nonmodular_rpm_units: Set[UbiUnit]  # all non modular rpm units
    modulemd_units: Optional[Set[UbiUnit]] = None
    modulemd_defaults: Optional[Set[UbiUnit]] = None


class NonModularAuditor:
    """
    A class to audit non-modular RPM units against whitelist and blacklist rules.

    Attributes:
        out_repo_id (str): The ID of the output repository.
        whitelist (Set[str]): A set of whitelisted package names.
        blacklist (Set[PackageToExclude]): A set of packages to exclude.
        in_units (Set[UbiUnit]): Input units to be audited.
        out_units (Set[UbiUnit]): Output units to be audited.
    """

    def __init__(
        self,
        out_repo_id: str,
        whitelist: Set[str],
        blacklist: Set[PackageToExclude],
        in_units: Set[UbiUnit],
        out_units: Set[UbiUnit],
    ) -> None:
        self.out_repo_id = out_repo_id
        self.whitelist = whitelist
        self.blacklist = blacklist
        self.in_units = in_units
        self.out_units = out_units

    def validate_versions(self) -> None:
        """
        Validates the versions of input and output RPM units.

        Logs a warning if an output unit is outdated compared to an input unit.
        """

        def log_warning(
            warn_tuple: tuple[str, tuple[str, str, str], tuple[str, str, str]],
        ) -> None:
            _LOG.warning(
                "[%s] UBI rpm '%s' is outdated (current: %s, latest: %s)",
                self.out_repo_id,
                *warn_tuple,
            )

        for out_unit in self.out_units:
            for in_unit in self.in_units:
                if (out_unit.name, out_unit.arch) == (in_unit.name, in_unit.arch):
                    out_evr = (out_unit.epoch, out_unit.version, out_unit.release)
                    in_evr = (in_unit.epoch, in_unit.version, in_unit.release)

                    if RELATION_CMP_MAP["LT"](
                        out_evr,
                        in_evr,
                    ):  # type: ignore
                        log_warning((out_unit.name, out_evr, in_evr))
                    break

    def check_content_rules(self) -> None:
        """
        Checks the content rules against the whitelist and blacklist.

        Calls the methods to verify the blacklist and whitelist.
        """
        self._verify_blacklist()
        self._verify_whitelist()

    def _verify_blacklist(self) -> None:
        """
        Verifies that no blacklisted packages are present in the output units.

        Logs a warning if any blacklisted packages are found.
        """
        blacklisted_pkgs = {
            u.name for u in self.out_units if is_blacklisted(u, list(self.blacklist))
        }

        if blacklisted_pkgs:
            _LOG.warning(
                "[%s] blacklisted content found in output repository;\n\t%s",
                self.out_repo_id,
                "\n\t".join(sorted(blacklisted_pkgs)),
            )

    def _verify_whitelist(self) -> None:
        """
        Verifies that whitelisted packages are present in the input and output units.

        Logs information and warnings based on the presence of whitelisted packages.
        """
        for whitelisted_pkg_name in self.whitelist:
            in_input_repos = any(
                unit.name == whitelisted_pkg_name for unit in self.in_units
            )
            in_output_repo = any(
                unit.name == whitelisted_pkg_name for unit in self.out_units
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


class ContentProcessor:
    """
    A class to process and audit repository content.

    Attributes:
        client (Client): The client used to interact with the repository.
        out_repo (YumRepository): The output repository.
        in_repos (List[YumRepository]): A list of input repositories.
        config_loader (UbiConfigLoader): The configuration loader.
        all_modular_filenames (Set[str]): A set of all modular filenames.
        is_out_modular (bool): Indicates if the output repository is modular.
        nonmodular_auditor (NonModularAuditor): Auditor for non-modular RPMs.
        out_repo_content (RepoContent): Content of the output repository.
        in_repos_content (RepoContent): Content of the input repositories.
        criteria (List[Criteria]): Search criteria for RPM units.
    """

    def __init__(
        self,
        client: Client,
        out_repo: YumRepository,
        in_repos: List[YumRepository],
        config_loader: UbiConfigLoader,
        all_modular_filenames: Set[str],
        is_out_modular: bool,
    ) -> None:
        self.client = client
        self.out_repo = out_repo
        self.in_repos = in_repos
        self.config_loader = config_loader
        self.all_modular_filenames = all_modular_filenames
        self.is_out_modular = is_out_modular

        self.nonmodular_auditor = NonModularAuditor(
            out_repo.id, set(), set(), set(), set()
        )
        self.out_repo_content = RepoContent(nonmodular_rpm_units=set())
        self.in_repos_content = RepoContent(nonmodular_rpm_units=set())
        self.criteria: List[Criteria] = []

    def process_and_audit(self) -> None:
        """
        Processes and audits the repository content.

        Fetches content from the output and input repositories, sets the
        whitelist and blacklist, and performs validation and checks.
        """
        if self.is_out_modular:
            _LOG.info(
                "Processing and auditing UBI repo '%s' with modular content...\n",
                self.out_repo.id,
            )
        elif "source" in self.out_repo.id:
            _LOG.info("Skipping source RPM: %s\n", self.out_repo.id)
            return
        else:
            _LOG.info("Processing and auditing UBI repo '%s'...\n", self.out_repo.id)
        self._fetch_out_repo_content()
        self._fetch_in_repos_contents()
        self._set_whitelist_blacklist()
        self.nonmodular_auditor.validate_versions()
        self.nonmodular_auditor.check_content_rules()

    def _fetch_out_repo_content(self) -> None:
        """
        Fetches the content of the output repository.

        Updates the nonmodular RPM units in the output repository content
        and the auditor.
        """
        future_rpm_units = search_units(
            self.out_repo, [Criteria.true()], RpmUnit, unit_fields=RPM_FIELDS
        )
        non_modular_rpms = set(
            get_n_latest_from_content(
                future_rpm_units.result(), modular_rpms=self.all_modular_filenames
            )
        )
        self.out_repo_content.nonmodular_rpm_units = non_modular_rpms
        self.nonmodular_auditor.out_units = self.out_repo_content.nonmodular_rpm_units

    def _fetch_in_repos_contents(self) -> None:
        """
        Fetches the contents of the input repositories.

        Updates the nonmodular RPM units in the input repositories content
        and the auditor.
        """
        for in_repo in self.in_repos:
            future_rpm_units = search_units(
                in_repo, [Criteria.true()], RpmUnit, unit_fields=RPM_FIELDS
            )
            non_modular_rpms = set(
                get_n_latest_from_content(
                    future_rpm_units.result(), modular_rpms=self.all_modular_filenames
                )
            )
            self.in_repos_content.nonmodular_rpm_units |= non_modular_rpms
        self.nonmodular_auditor.in_units = self.in_repos_content.nonmodular_rpm_units

    def _set_whitelist_blacklist(self) -> None:
        """
        Sets the whitelist and blacklist based on the configuration of input repositories.

        Updates the auditor's whitelist and blacklist attributes.
        """
        for in_repo in self.in_repos:
            config = get_content_config(
                self.config_loader,
                in_repo.content_set,
                self.out_repo.content_set,
                self.out_repo.ubi_config_version,
            )

            self.nonmodular_auditor.blacklist |= set(parse_blacklist_config(config))
            pkg_whitelist, debuginfo_whitelist = filter_whitelist(
                config, list(self.nonmodular_auditor.blacklist)
            )
            if "debug" in self.out_repo.id:
                self.nonmodular_auditor.whitelist |= debuginfo_whitelist
            else:
                self.nonmodular_auditor.whitelist |= pkg_whitelist
