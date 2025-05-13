from pubtools.pulplib import Client, Criteria, Matcher, YumRepository

from ubi_manifest.worker.tasks.auditing import ContentProcessor
from ubi_manifest.worker.tasks.celery import app
from ubi_manifest.worker.ubi_config import UbiConfigLoader
from ubi_manifest.worker.utils import make_pulp_client


def fetch_ubi_repos_bundles(
    client: Client,
) -> dict[str, dict[str, YumRepository]]:
    """
    Helper function that fetches repos and bundles them
    by binary, source and debug type, using the binary
    repo as the driver for querying Pulp.
    """
    population_criterion = Criteria.with_field("ubi_population", True)
    is_binary_criterion = Criteria.with_field(
        "id", Matcher.regex(r"^(?!.*(?:debug|source)).*$")
    )
    search_criteria = Criteria.and_(population_criterion, is_binary_criterion)
    ubi_bin_repos: list[YumRepository] = list(
        client.search_repository(search_criteria).result()
    )
    ubi_repos_bundle = {}
    for bin_repo in ubi_bin_repos:
        ubi_repos_bundle[bin_repo.id] = {
            "bin_repo": bin_repo,
            "source_repo": bin_repo.get_source_repository().result(),
            "debug_repo": bin_repo.get_debug_repository().result(),
        }

    return ubi_repos_bundle


@app.task  # type: ignore
def content_audit_task() -> None:
    """
    This task checks that all available content is up-to-date, that whitelisted
    content is present, and that blacklisted content is absent.
    """
    config_loaders_map = {
        repo_class: UbiConfigLoader(url)
        for repo_class, url in app.conf.content_config.items()
    }

    with make_pulp_client(app.conf) as client:
        out_repos_bundles = fetch_ubi_repos_bundles(client)
        for bin_repo_id, out_repo_bundle in out_repos_bundles.items():
            bin_criterion = Criteria.with_id(
                out_repo_bundle["bin_repo"].population_sources
            )
            source_criterion = Criteria.with_id(
                out_repo_bundle["source_repo"].population_sources
            )
            debug_criterion = Criteria.with_id(
                out_repo_bundle["debug_repo"].population_sources
            )
            in_repos_bundle = {
                "bin_repos": list(client.search_repository(bin_criterion).result()),
                "source_repos": list(
                    client.search_repository(source_criterion).result()
                ),
                "debug_repos": list(client.search_repository(debug_criterion).result()),
            }

            current_loader = next(
                (
                    loader
                    for repo_class, loader in config_loaders_map.items()
                    if repo_class in bin_repo_id
                ),
                None,
            )
            if not current_loader:
                raise ValueError(
                    f"Repository {bin_repo_id} is set for ubi_population but has unexpected id."
                )

            content_processor = ContentProcessor(
                client,
                out_repo_bundle,
                in_repos_bundle,
                current_loader,
            )
            content_processor.process_and_audit_bundle()
