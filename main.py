import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

import libzfs

from cephlibs.rados import Ioctx

try:
    import rados
    import rbd
except ModuleNotFoundError:
    import cephlibs.rados as rados
    import cephlibs.rbd as rbd


def zfs_snapshot_name(snap: libzfs.ZFSSnapshot) -> str:
    return snap.name.split('@')[-1]


def zfs_snapshot_created_time(snap: libzfs.ZFSSnapshot) -> datetime:
    return snap.properties['creation'].parsed


class ZfsContext:
    def __init__(self, base_dataset: libzfs.ZFSDataset):
        self._base = base_dataset

    def get_child(self, name: str) -> Optional[libzfs.ZFSDataset]:
        for child in self._base.children:
            if child.name.split('/')[-1] == name:
                return child
        return None

    def create_child_vol(self, name: str, size: int) -> libzfs.ZFSDataset:
        pool: libzfs.ZFSPool = self._base.pool
        # TODO: i think this returns nothing
        print(f'Creating child volume {self._base.name}/{name} with capacity {size}B')
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


class ZfsDatasetContext:

    def __init__(self, base: ZfsContext, name: str):
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

    def log(self, msg):
        print(f'[{self.name}] {msg}')

    def prepare(self, snapshot: Optional[str | libzfs.ZFSSnapshot], required_size: int):
        ds = self.volume

        if ds is None:
            print(f'Dataset {self.zfs_path} does not exist - creating')
            ds = self._base.create_child_vol(self.name, required_size)
            print(f'Created {self.zfs_path}, waiting for {self.device_node} to exist...')
            while not os.path.exists(self.device_node):
                time.sleep(0.5)

        elif ds.type != libzfs.DatasetType.VOLUME:
            raise RuntimeError(f'Dataset for {self.zfs_path} exists but is not a volume!')

        if snapshot is not None:
            if isinstance(snapshot, str):
                snapshot = self.get_snapshot_by_name(snapshot)
            print(f'Rolling back to {snapshot.name}')
            snapshot.rollback()
            pass

        existing_size = ds.properties['volsize'].parsed
        if existing_size < required_size:
            delta = required_size - existing_size
            print(
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


def do_backup(ceph_rbd_image: rbd.Image, zfs_dest: ZfsDatasetContext):
    try:
        # This is an incremental backup when possible, else full backup.
        # First, we need to figure out our snapshot to use as a basis for incremental (or lack thereof).
        src_snaps: list[dict] = list(ceph_rbd_image.list_snaps())
        src_snap_names: list[str] = [snap['name'] for snap in src_snaps]
        # print(f'Source snapshots: {src_snap_names}')
        dest_snaps: list[libzfs.ZFSSnapshot] = zfs_dest.all_snapshots
        dest_snap_names: list[str] = [zfs_snapshot_name(snap) for snap in dest_snaps]
        # print(f'Dest snapshots: {dest_snap_names}')
        common_snaps: list[str] = [snap_name for snap_name in dest_snap_names if snap_name in src_snap_names]
        if common_snaps:
            latest_common_snap = common_snaps[-1]
        else:
            latest_common_snap = None

        now = datetime.utcnow()
        now_fmt = now.strftime('%Y-%m-%d-%H:%M:%S')
        new_snap_name = 'ctz-' + now_fmt
        img_name = ceph_rbd_image.get_name()
        if latest_common_snap is None:
            print(f'[{img_name}] Full backup: -> {new_snap_name}')
        else:
            print(f'[{img_name}] Incremental backup: {latest_common_snap} -> {new_snap_name}')
        print('[{img_name}] Creating RBD snapshot')
        ceph_rbd_image.create_snap(new_snap_name)
        print('[{img_name}] Created RBD snapshot')
        # set_snap sets the snapshot to read from via our client
        ceph_rbd_image.set_snap(new_snap_name)

        img_bytes = ceph_rbd_image.size()
        print(f'[{img_name}] Image size: {img_bytes}')

        zfs_dest.prepare(latest_common_snap, img_bytes)

        dev_path = zfs_dest.device_node
        print(f'[{img_name}] Going to write to {dev_path}')

        # TODO: does overwriting with the same data use extra space in ZFS?
        requested = [0]
        written = [0]
        failures = []
        with open(dev_path, 'rb+', 1024 * 1024 * 64, closefd=True) as dev:
            def callback_inner(offset: int, length: int, exists: bool):
                # time.sleep(1)
                print(f'[{img_name}] Thread: {threading.get_ident()}')
                requested[0] += length
                try:
                    dev.seek(offset, os.SEEK_SET)
                    dev.write(ceph_rbd_image.read(offset, length, 0))
                except Exception as e:
                    print(
                        f'[{img_name}]: FAILED WRITE - {length} bytes from {offset} to {offset + length - 1} (exists: {exists})\n{e}')
                    failures.append(e)
                    raise
                print(
                    f'[{img_name}]: Successful write - {length} bytes from {offset} to {offset + length - 1} (exists: {exists})')
                written[0] += length

            # This is a third party function which calls 'callback' repeatedly
            ceph_rbd_image.diff_iterate(
                offset=0,
                # Length can be larger than needed
                length=(2 ** 62) - 1,
                from_snapshot=latest_common_snap,
                iterate_cb=callback_inner,
                include_parent=True,
                whole_object=False
            )

            print(f'[{img_name}] Flushing')
            dev.flush()
            print(f'[{img_name}] Flushed')

        print(f'[{img_name}] Done')
        print(f'[{img_name}] Wrote {written[0]}/{requested[0]} bytes to {dev_path}')
        if failures:
            raise Exception(f'There were {len(failures)} failure(s)!!! Not snapshotting!')
        print(f'[{img_name}] Creating snapshot {zfs_dest.zfs_path}@{new_snap_name}')
        zfs_dest.create_snapshot(new_snap_name)
        print(f'[{img_name}] Created snapshot {zfs_dest.zfs_path}@{new_snap_name}')
    except Exception as e:
        print(f'[Error in {ceph_rbd_image.get_name}: {e}')
        raise


class BackupController:
    def __init__(self, ceph_pool: Ioctx, zfs_dest: ZfsContext):
        self.ceph_pool = ceph_pool
        self.zfs_dest = zfs_dest
        self.rbd = rbd.RBD()

    @property
    def all_image_names(self) -> list[str]:
        return self.rbd.list(self.ceph_pool)

    def should_backup_image(self, image_name: str) -> bool:
        # return image_name == 'ceph-test-2'
        return True

    @property
    def images_to_back_up(self) -> list[rbd.Image]:
        return [rbd.Image(self.ceph_pool, image_name, read_only=False) for image_name in self.all_image_names if
                self.should_backup_image(image_name)]

    def backup_all_images(self):
        images = self.images_to_back_up
        print(f'Going to back up {len(images)} images')
        with ThreadPoolExecutor(max_workers=16) as pool:
            for image in images:
                zdc = ZfsDatasetContext(self.zfs_dest, image.get_name())
                print(f'Backing up image {image.get_name()} to {zdc.zfs_path}')
                pool.submit(lambda: do_backup(image, zdc))
            pool.shutdown(wait=True, cancel_futures=False)
        print('Done with all')


# Connect to cluster
cluster = rados.Rados(name='client.backups', conffile='/etc/ceph/ceph.conf', clustername='ceph')
cluster.connect()
# List pools
# Open pool
ctx: Ioctx = cluster.open_ioctx('vmstorage')

z = libzfs.ZFS()

# img_name = img.get_name()
zc = ZfsContext(z.get_dataset('testpool/ceph-img-test'))
# zdc = ZfsDatasetContext(zc, img_name)
#
# do_backup(img, zdc)

bc = BackupController(ctx, zc)
bc.backup_all_images()

# TODO: confirmations for potentially bad things:
# - reverting to common snapshot
# - expanding image
