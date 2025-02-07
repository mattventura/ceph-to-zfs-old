# Obsolete - This Project is Being Rewritten

See [Ceph-to-ZFS](https://github.com/mattventura/ceph-to-zfs)

# Backup CephFS + RBD to ZFS

## Current Status

- RBD: Basic snapshotting and backup works
- CephFS: Not started

I love ZFS. One of the things I love about it is how easy it is to have high-quality backups of all of your ZFS data.
Projects like [zrepl](https://github.com/zrepl/zrepl) can serve as a backup software solution for your ZFS filesystems
and volumes. Plus, ZFS has native encryption support, and several features such as deduplication that can reduce the
size of your backups.

However, if you want a clustered filesystem like Ceph, there just isn't anything quite as good on the backup front.
I looked at the [Backy2](https://backy2.com/) project. It has most of what you'd want for managing Ceph volume backups.
It does encryption and deduplication, and only sends dirty blocks. However, you then need another solution, such as
[45Drives' cephgeorep](https://github.com/45Drives/cephgeorep) to handle CephFS. There isn't a comprehensive solution
for both RBD and CephFS.

The goal of this project is to provide a single solution for CephFS + RBD that uses ZFS as its target. This means that
you can get the benefits of ZFS (dedup, encryption, and more), without needing to manage code for all of that.

The basic flow for RBD backup is:

1. Look at the latest common snapshot between the source RBD volume and target ZFS zvol.
2. Create a new snapshot of the RBD volume.
3. Get the dirty blocks between the new snapshot and the latest common snapshot (if it exists, else the entire drive).
4. Write the blocks to the target zvol device.
5. Snapshot the target zvol, with a name matching the RBD snapshot.

For CephFS:

1. Look at the timestamp of the latest snapshot on the target ZFS filesystem.
2. Using the `rctime` attribute, identify changed files.
3. Mount the target FS.
4. Write the modified files.
5. Snapshot the target FS.

## Advantages

This project aims to, in the long term, become an improvement over the alternatives in these areas:

- Covers both CephFS and RBD (potentially RGW in the future if I can find a way to make it work well).
- Allows the use of native ZFS features such as encryption and deduplication.
- In addition, use of ZFS means you would be able to easily mount and access old snapshots, without having to restore.

In the long term, I am to make the system work in a similar way to Zrepl, where it supports pushing to a remote daemon,
wildcard selection of filesystems and volumes, and has a UI for viewing backup status.