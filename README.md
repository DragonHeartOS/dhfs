# dhfs

The dhfs filesystem is a 64-bit FAT-like filesystem which aims to be extremely
simple to implement.

Keep in mind that this is still a work in progress, and the specification might change.
I'll try to keep everything backwards compatible (in a clean way)
when I add new features or make modifications to the filesystem.

In this repo you can find the full specification in the `spec.txt` file,
and a utility to manipulate the filesystem (`dhfs-utils`).
You can compile and install the `dhfs-utils` program using `make` the usual way.

A FUSE implementation of a filesystem driver named `dhfs-fuse` is also provided (thanks to Geertiebear).

NOTE: dhfs is based off of echfs.

# Build dependencies

`dhfs-fuse` depends on `libuuid`, `libfuse`, and `pkg-config`. (On Debian/Ubuntu based distros,
the packages are called `uuid-dev`, `libfuse-dev`, and `pkg-config`, respectively).

On systems where FUSE is not available, it is possible to compile `dhfs-utils`
exclusively by running `make utils` instead of `make` and `make install-utils`
instead of `make install`.

# Building

```
make
sudo make install
```

# Usage

## dhfs-utils

dhfs-utils is used as ``dhfs-utils <flags> <image> <command> <command args...>``, where
a command can be any of the following:

* ``import``, which copies to the image with args ``<source> <destination>``
* ``export``, which copies from the image  with args ``<source> <destination>``
* ``ls``, with arg ``<path>`` (can be left empty), it lists the files in the path or
 root if the path is not specified
* ``mkdir``, with arg ``<path>``, makes a directory with the specified path.
* ``format``, with arg ``<block size>`` formats the image
* ``quick-format`` with arg ``<block size>`` formats the image

There are also several flags you can specify

* ``-f`` ignore existing file errors on ``import``
* ``-m`` specify that the image is MBR formatted
* ``-g`` specify that the image is GPT formatted
* ``-p <part>`` specify which partition the dhfs image is in
* ``-v`` be verbose

## dhfs-fuse

dhfs-fuse is used as ``dhfs-fuse <flags> <image> <mountpoint>``, with the following flags:

* ``-m`` specify that the image is MBR formatted
* ``-g`` specify that the image is GPT formatted
* ``-p <part>`` specify which partition the dhfs image is in
* ``-d`` run in debug mode (don't detach)

## Creating a filesystem

A filesystem can be created with the following commands
```
dd if=/dev/zero of=image.hdd bs=4M count=128
parted -s image.hdd mklabel msdos
parted -s image.hdd mkpart primary 2048s 100%
dhfs-utils -m -p0 image.hdd quick-format 512
```
