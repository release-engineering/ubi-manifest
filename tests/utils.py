from pubtools.pulplib import YumRepository


def create_and_insert_repo(**kwargs):
    pulp = kwargs.pop("pulp")
    pulp.insert_repository(YumRepository(**kwargs))

    return pulp.client.get_repository(kwargs["id"])
