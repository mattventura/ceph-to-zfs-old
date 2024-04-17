from ceph_to_zfs.configuration_options import *

cluster = CephCluster(
    auth_name='client.backups',
    conf_file='/etc/ceph/ceph.conf',
    cluster_name='ceph'
)

pool = PoolConfig(
    ceph_pool_name='vmstorage',
    zfs_destination='testpool/ceph-img-test',
)

jobs: list[Job] = [
    Job(name='Backup VM Images', cluster=cluster, pools=[pool])
]
