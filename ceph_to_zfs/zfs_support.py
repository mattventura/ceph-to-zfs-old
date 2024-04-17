import os
import time
from datetime import datetime
from typing import Optional

import libzfs

from ceph_to_zfs.statuslogger import Loggable, JobLogger

def zfs_snapshot_name(snap: libzfs.ZFSSnapshot) -> str:
    return snap.name.split('@')[-1]


def zfs_snapshot_created_time(snap: libzfs.ZFSSnapshot) -> datetime:
    return snap.properties['creation'].parsed



class ZfsContext(Loggable):
    def __init__(self, status_logger: JobLogger, base_dataset: libzfs.ZFSDataset):
        super().__init__(status_logger)
        self._base = base_dataset

    def get_child(self, name: str) -> Optional[libzfs.ZFSDataset]:
        for child in self._base.children:
            if child.name.split('/')[-1] == name:
                return child
        return None

    def create_child_vol(self, name: str, size: int) -> libzfs.ZFSDataset:
        pool: libzfs.ZFSPool = self._base.pool
        # TODO: i think this returns nothing
        self.log(f'Creating child volume {self._base.name}/{name} with capacity {size}B')
        pool.create(
            self._base.name + '/' + name,
            fsopts={
                # TODO
                'volsize': str(size),
            },
            fstype=libzfs.DatasetType.VOLUME,
            sparse_vol=True)
        return self.get_child(name)

    @property
    def zfs_path(self) -> str:
        return self._base.name


class ZfsDatasetContext(Loggable):

    def __init__(self, status_logger: JobLogger, base: ZfsContext, name: str):
        super().__init__(status_logger)
        self._base = base
        self.name = name

    @property
    def volume(self) -> Optional[libzfs.ZFSDataset]:
        return self._base.get_child(self.name)

    @property
    def all_snapshots(self) -> list[libzfs.ZFSSnapshot]:
        existing = self.volume
        if existing is not None:
            snaps: list[libzfs.ZFSSnapshot] = list(existing.snapshots)
            snaps.sort(key=zfs_snapshot_created_time)
            return snaps
        else:
            return []

    def get_snapshot_by_name(self, name: str) -> libzfs.ZFSSnapshot:
        for snapshot in self.all_snapshots:
            if zfs_snapshot_name(snapshot) == name:
                return snapshot
        raise KeyError(f'Dataset {self.zfs_path} does not have a snapshot with name "{name}"')

    def prepare(self, snapshot: Optional[str | libzfs.ZFSSnapshot], required_size: int):
        self.set_status('Preparing Target Zvol')
        ds = self.volume

        if ds is None:
            self.set_status('Creating Target Zvol')
            self.log(f'Dataset {self.zfs_path} does not exist - creating')
            ds = self._base.create_child_vol(self.name, required_size)
            self.log(f'Created {self.zfs_path}, waiting for {self.device_node} to exist...')
            while not os.path.exists(self.device_node):
                time.sleep(0.5)

        elif ds.type != libzfs.DatasetType.VOLUME:
            raise RuntimeError(f'Dataset for {self.zfs_path} exists but is not a volume!')

        if snapshot is not None:
            self.set_status('Rolling Zvol back to snapshot')
            if isinstance(snapshot, str):
                snapshot = self.get_snapshot_by_name(snapshot)
            self.log(f'Rolling back to {snapshot.name}')
            snapshot.rollback()
            pass

        existing_size = ds.properties['volsize'].parsed
        if existing_size < required_size:
            self.set_status('Expanding Zvol')
            delta = required_size - existing_size
            self.log(
                f'Resizing volume from {existing_size} to {required_size} (increase of {delta}B)')
            ds.properties['volsize'].value = required_size

    @property
    def zfs_path(self) -> str:
        return self._base.zfs_path + '/' + self.name

    @property
    def device_node(self) -> str:
        return f'/dev/zvol/{self.volume.name}'

    def create_snapshot(self, new_snap_name: str):
        return self.volume.snapshot(self.zfs_path + '@' + new_snap_name)

