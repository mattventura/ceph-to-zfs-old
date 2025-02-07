import os
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import libzfs

from ceph_to_zfs import statuslogger
from ceph_to_zfs.zfs_support import ZfsDatasetContext, zfs_snapshot_name, ZfsContext
from ceph_to_zfs.configuration_options import ImageFilter

try:
    import rados
    import rbd
except ModuleNotFoundError:
    print('Using backup ceph libs - please install python3-ceph')
    import cephlibs.rados as rados
    import cephlibs.rbd as rbd

from ceph_to_zfs.statuslogger import JobLogger, Loggable, Failed


def format_exception(e: Exception) -> str:
    return ''.join(traceback.format_exception(e.__class__, e, e.__traceback__))


def do_backup(log: JobLogger, ceph_rbd_image: rbd.Image, zfs_dest: ZfsDatasetContext):
    log.status_text = 'Calculating backup'
    log.status_type = statuslogger.In_Progress
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
            log.log(f'Full backup: -> {new_snap_name}')
        else:
            log.log(f'Incremental backup: {latest_common_snap} -> {new_snap_name}')
        log.log_status('Creating RBD snapshot')
        ceph_rbd_image.create_snap(new_snap_name)
        log.log('Created RBD snapshot')
        # set_snap sets the snapshot to read from via our client
        ceph_rbd_image.set_snap(new_snap_name)

        img_bytes = ceph_rbd_image.size()
        log.log(f'Image size: {img_bytes}')

        zfs_dest.prepare(latest_common_snap, img_bytes)

        dev_path = zfs_dest.device_node
        log.log(f'Going to write to {dev_path}')

        # TODO: does overwriting with the same data use extra space in ZFS?
        requested = [0]
        written = [0]
        failures = []
        # TODO: if the system is slow, this will not work correctly, as udev can't fix disk permissions fast enough
        # with open(dev_path, 'rb+', 1024 * 1024 * 64, closefd=True) as dev:
        with open(dev_path, 'rb+', 0, closefd=True) as dev:
            def callback_inner(offset: int, length: int, exists: bool):
                # time.sleep(1)
                # print(f'[{img_name}] Thread: {threading.get_ident()}')
                requested[0] += length
                try:
                    dev.seek(offset, os.SEEK_SET)
                    dev.write(ceph_rbd_image.read(offset, length, 0))
                    dev.flush()
                except Exception as e:
                    log.log(
                        f'FAILED WRITE - {length} bytes from {offset} to {offset + length - 1} (exists: {exists})\n{e}')
                    failures.append(e)
                    raise
                # print(
                #     f'[{img_name}]: Successful write - {length} bytes from {offset} to {offset + length - 1} (exists: {exists})')
                written[0] += length

            log.log_status('Writing data')
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

            log.log_status('Flushing')
            dev.flush()
            log.log_status(f'Flushed')

        if failures:
            log.log_status(f'FAILED! One or more writes failed, see log. Wrote {written[0]}/{requested[0]} bytes to {dev_path}')
            raise Exception(f'There were {len(failures)} failure(s)!!! Not snapshotting!')
        else:
            log.log_status(f'Finished writing {written[0]}/{requested[0]} bytes to {dev_path}')

        log.log_status(f'Creating snapshot {zfs_dest.zfs_path}@{new_snap_name}')
        zfs_dest.create_snapshot(new_snap_name)
        log.log_status(f'Finalized destination snapshot {zfs_dest.zfs_path}@{new_snap_name}', statuslogger.Success)
    except Exception as e:
        log.log_status(f'FAILED! {e}', statuslogger.Failed)
        log.log(f'Error in {ceph_rbd_image.get_name()}: {format_exception(e)}')
        raise


class PoolBackupController(Loggable):
    def __init__(self, logger: JobLogger, ceph_pool: rados.Ioctx, zfs_dest: ZfsContext, image_filter: ImageFilter):
        super().__init__(logger)
        self.ceph_pool = ceph_pool
        self.zfs_dest = zfs_dest
        self.rbd = rbd.RBD()
        self.image_filter = image_filter

    @property
    def all_image_names(self) -> list[str]:
        # noinspection PyTypeChecker
        return self.rbd.list(self.ceph_pool)

    def should_backup_image(self, image_name: str) -> bool:
        return self.image_filter.should_backup(image_name)

    @property
    def images_to_back_up(self) -> list[rbd.Image]:
        return [rbd.Image(self.ceph_pool, image_name, read_only=False) for image_name in self.all_image_names if
                self.should_backup_image(image_name)]

    def backup_all_images(self):
        images = self.images_to_back_up
        self.log(f'Going to back up {len(images)} images')
        with ThreadPoolExecutor(max_workers=2) as pool:
            for image in images:
                image_context = self.logger.make_or_replace_child(image.get_name(), True)
                image_context.status_text = 'Starting'
                zdc = ZfsDatasetContext(image_context, self.zfs_dest, image.get_name())
                image_context.log_status(f'Backing up image {image.get_name()} to {zdc.zfs_path}')

                def backf(image_context=image_context, image=image, zdc=zdc):
                    try:
                        do_backup(image_context, image, zdc)
                    except Exception as e:
                        image_context.log_status(f"Image {image.get_name()} failed! Exception: {e}", Failed)

                pool.submit(backf)
            pool.shutdown(wait=True, cancel_futures=False)
        self.log_status('Complete')
