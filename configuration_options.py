import abc
import dataclasses
import re


class ImageFilter(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def should_backup(self, image_name: str):
        raise NotImplemented


class AllImagesFilter(ImageFilter):
    def should_backup(self, image_name: str):
        return True


class RegexImagesFilter(ImageFilter):
    def __init__(self, pattern: str):
        self.pattern = re.compile(pattern)

    def should_backup(self, image_name: str):
        return self.pattern.match(image_name) is not None


@dataclasses.dataclass(kw_only=True, frozen=True)
class CephCluster:
    auth_name: str = 'client.admin'
    conf_file: str = '/etc/ceph/ceph.conf'
    cluster_name: str = 'ceph'


@dataclasses.dataclass(kw_only=True, frozen=True)
class PoolConfig:
    ceph_pool_name: str
    zfs_destination: str
    image_filter: ImageFilter = AllImagesFilter()


# TODO: not implemented yet
@dataclasses.dataclass(kw_only=True, frozen=True)
class MultiPoolConfig:
    ceph_pool_name: str
    zfs_destination: str


@dataclasses.dataclass(kw_only=True, frozen=True)
class Job:
    name: str
    cluster: CephCluster
    pools: list[PoolConfig]
