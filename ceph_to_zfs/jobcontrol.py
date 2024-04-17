import importlib.util
import importlib.util

import libzfs

from ceph_to_zfs.backup import PoolBackupController
from ceph_to_zfs.configuration_options import Job, PoolConfig
from ceph_to_zfs.statuslogger import *
from ceph_to_zfs.zfs_support import ZfsContext

try:
    import rados
    import rbd
except ModuleNotFoundError:
    print('Using backup ceph libs - please install python3-ceph')
    import cephlibs.rados as rados
    import cephlibs.rbd as rbd

# TODO: lazy init this
z = libzfs.ZFS()


class JobControl:

    def __init__(self, job: Job, job_logger: JobLogger):
        self.job = job
        self.job_logger = job_logger

    def run(self):
        job_logger = self.job_logger
        try:
            job = self.job
            job_logger.status_type = Preparing
            job_logger.status = 'Connecting to cluster'
            cc = job.cluster
            with rados.Rados(name=cc.auth_name, conffile=cc.conf_file, clustername=cc.cluster_name) as cluster:
                job_logger.status = 'In progress'
                pools: list[PoolConfig] = job.pools
                # TODO: parallelize
                for pool in pools:
                    pool_logger = job_logger.make_or_replace_child(pool.ceph_pool_name, True)
                    pool_logger.log_status('Starting pool backup', In_Progress)
                    with cluster.open_ioctx(pool.ceph_pool_name) as ctx:
                        pool_logger.status = 'In progress'
                        # img_name = img.get_name()
                        zc = ZfsContext(pool_logger, z.get_dataset(pool.zfs_destination))
                        bc = PoolBackupController(pool_logger, ctx, zc, pool.image_filter)
                        bc.backup_all_images()
                        pool_logger.status = 'Complete'
                        pool_logger.status_type = Success
                job_logger.status = 'Complete'
                job_logger.status_type = Success
        except Exception as e:
            job_logger.log(f'Failure: {e}')
            job_logger.log_status('Failed!', status_type=Failed)


class GlobalControl:

    def __init__(self, logger: TopLevelLogger, config):
        self.logger = logger
        self.jobs: list[JobControl] = [self._make_or_replace_child(job) for job in config.jobs]
        logger.log_status('Ready to run jobs', status_type=Not_Started)

    @classmethod
    def from_config_file(cls, config_file: str):
        top_level_logger = TopLevelLogger('Global')
        top_level_logger.log_status('Loading configuration', status_type=Not_Started)
        spec = importlib.util.spec_from_file_location('config', config_file)
        config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module=config)
        return cls(top_level_logger, config)

    def activate_loop(self):
        # Start scheduled task loop
        # TODO implement all of this
        pass

    def run_all_jobs(self):
        jobs = self.jobs
        # TODO: parallelize
        try:
            self.logger.log_status('Running jobs', status_type=In_Progress)
            for job in jobs:
                job.run()
            self.logger.log_status('Complete', status_type=Success)
        except Exception as e:
            self.logger.log(f"Error! {e}")
            self.logger.log_status('Failed', status_type=Failed)

    def _make_or_replace_child(self, job: Job) -> JobControl:
        job_logger = self.logger.make_or_replace_child(job.name, False)
        jc = JobControl(job, job_logger)
        return jc
