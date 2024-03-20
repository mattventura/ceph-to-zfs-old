# Ceph Prep

First, configure the host with an appropriate Ceph cluster connection which has adequate permissions.

TODO write more about said permissions.

# ZFS Prep

Create a single parent dataset for each area that you wish to back up (one for RBD, one for CephFS).
Ensure that the user under which you will be running the software has permission to:
- create children underneath the parent dataset,
- change properties of said children, and
- roll back the children to a snapshot version.

You will most likely wish to enable compression on this dataset, as it will significantly reduce the space needed.
Configure other ZFS properties, such as deduplication or encryption.

## ZVol Prep

In addition, the user who will be running will need read/write access to zvol device nodes. 
The first issue is permissions.
The quick-and-dirty but less secure option is to add the user to whichever group owns the `/dev/zd*` nodes. On 
Debian-based systems, this is usually called `disk`. This will only work if the device nodes are group-writable.
The safer option is to use udev rules (or whatever mechanism your OS provides) to control access in a more fine-grained
manner. 

You will also need to ensure that you don't have other storage subsystems such as mdraid lvm (which may have been 
pulled in as a dependency of Ceph depending on how you installed it). If these start poking around in your ZVols, then
they may cause unwanted locking. You should consult the documentation for each of these to find out how to limit
its scanning as needed. Or, if they are not needed for any other purpose, completely disable or uninstall them.