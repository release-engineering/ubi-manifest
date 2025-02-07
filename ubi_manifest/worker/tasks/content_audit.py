from concurrent.futures import Future
from typing import List

from pubtools.pulplib import Client, Criteria, Page, YumRepository

from ubi_manifest.worker.common import get_pkgs_from_all_modules
from ubi_manifest.worker.tasks.auditing import ContentProcessor
from ubi_manifest.worker.tasks.celery import app
from ubi_manifest.worker.ubi_config import UbiConfigLoader
from ubi_manifest.worker.utils import make_pulp_client


def fetch_from_future_page(client: Client, criteria: Criteria) -> List[YumRepository]:
    """
    Fetches repositories based on the given criteria and returns them as a list.

    Args:
        client: The Pulp client to use for fetching repositories.
        criteria: The criteria to filter repositories.

    Returns:
        A list of YumRepository objects.
    """

    future_page: Future[Page] = client.search_repository(criteria)
    return list(future_page.result())


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
        out_repos: List[YumRepository] = fetch_from_future_page(
            client, Criteria.with_field("ubi_population", True)
        )
        all_modular_filenames = get_pkgs_from_all_modules(out_repos)
        all_binary_in_repos_ids = set()
        for repo in out_repos:
            if "debug" not in repo.id and "source" not in repo.id:
                all_binary_in_repos_ids.update(repo.population_sources)
        all_binary_in_repos = fetch_from_future_page(
            client, Criteria.with_id(all_binary_in_repos_ids)
        )
        all_modular_filenames.update(get_pkgs_from_all_modules(all_binary_in_repos))
        for out_repo in out_repos:
            is_out_modular = "debug" not in out_repo.id and "source" not in out_repo.id
            in_repos: List[YumRepository] = fetch_from_future_page(
                client, Criteria.with_id(out_repo.population_sources)
            )

            current_loader = None
            for repo_class, loader in config_loaders_map.items():
                if repo_class in out_repo.id:
                    current_loader = loader
                    break

            if not current_loader:
                raise ValueError(
                    f"Repository {out_repo!r} is set for ubi_population but has unexpected id."
                )

            content_processor = ContentProcessor(
                client,
                out_repo,
                in_repos,
                current_loader,
                all_modular_filenames,
                is_out_modular,
            )
            content_processor.process_and_audit()
