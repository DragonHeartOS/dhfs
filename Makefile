CC=cc
OBJCOPY=objcopy
DESTDIR=
PREFIX=/usr/local
CFLAGS=-O3 -Wall -Wextra -pipe

.PHONY: all clean install-fuse install-utils install-mkfs install

all: dhfs-utils dhfs-fuse mkfs.dhfs

boot.bin: boot.asm
	nasm -fbin -o boot.bin boot.asm

boot.o: boot.bin
	$(OBJCOPY) -B i8086 -I binary -O default boot.bin boot.o

dhfs-utils: dhfs-utils.c part.c part.h
	$(CC) $(CFLAGS) part.c dhfs-utils.c -luuid -o dhfs-utils

dhfs-fuse: dhfs-fuse.c part.c part.h
	$(CC) $(CFLAGS) part.c dhfs-fuse.c $(shell pkg-config fuse --cflags --libs) -o dhfs-fuse

mkfs.dhfs: boot.o mkfs.dhfs.c
	$(CC) $(CFLAGS) boot.o mkfs.dhfs.c -o mkfs.dhfs

clean:
	rm -f dhfs-utils
	rm -f dhfs-fuse
	rm -f mkfs.dhfs
	rm -f boot.bin boot.o

install-mkfs: mkfs.dhfs
	install -d $(DESTDIR)$(PREFIX)/bin
	install -s mkfs.dhfs $(DESTDIR)$(PREFIX)/bin

install-utils: dhfs-utils
	install -d $(DESTDIR)$(PREFIX)/bin
	install -s dhfs-utils $(DESTDIR)$(PREFIX)/bin

install-fuse: dhfs-fuse
	install -d $(DESTDIR)$(PREFIX)/bin
	install -s dhfs-fuse $(DESTDIR)$(PREFIX)/bin
	ln -sf $(DESTDIR)$(PREFIX)/bin/dhfs-fuse $(DESTDIR)$(PREFIX)/bin/mount.dhfs-fuse
	ln -sf $(DESTDIR)$(PREFIX)/bin/dhfs-fuse $(DESTDIR)$(PREFIX)/bin/mount.dhfs

install: install-utils install-fuse install-mkfs
