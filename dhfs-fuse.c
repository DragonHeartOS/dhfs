#define FUSE_USE_VERSION 29

#include <fuse.h>
#include <string.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <assert.h>
#include <errno.h>
#include <sys/time.h>

#include "part.h"

#define RESERVED_BLOCKS 16
#define SEARCH_FAILURE          0xffffffffffffffff
#define ROOT_ID                 0xffffffffffffffff
#define BYTES_PER_SECT          512
#define ENTRIES_PER_SECT        2
#define FILENAME_LEN            201
#define FILE_TYPE               0
#define DIRECTORY_TYPE          1
#define DELETED_ENTRY           0xfffffffffffffffe
#define RESERVED_BLOCK          0xfffffffffffffff0
#define END_OF_CHAIN            0xffffffffffffffff

//max handles for now
#define MAX_HANDLES 1024
#define MAX_PATH_LEN 4096

struct entry_t {
    uint64_t parent_id;
    uint8_t type;
    char name[FILENAME_LEN];
    uint64_t atime;
    uint64_t mtime;
    uint64_t payload;
    uint64_t size;
}__attribute__((packed));

struct path_result_t {
    uint64_t target_entry;
    struct entry_t target;
    struct entry_t parent;
    char name[FILENAME_LEN];
    char path[MAX_PATH_LEN];
    int failure;
    int not_found;
    uint8_t type;
    struct path_result_t *next;
};

struct dhfs_handle_t {
    uint8_t occupied;
    char path[MAX_PATH_LEN];
    int flags;
    struct path_result_t *path_res;
    uint64_t *alloc_map;
    uint64_t total_blocks;
};

struct path_result_table {
    struct path_result_t **table;
    uint64_t size;
    uint64_t num_elements;
};

static struct dhfs {
    char *image_path;
    char *mountpoint;
    struct fuse_chan *chan;
    struct fuse_session *session;
    int mbr, gpt, partition;
    uint64_t part_offset;

    FILE *image;
    uint64_t image_size;
    uint64_t blocks;
    uint64_t fat_size;
    uint64_t fat_start;
    uint64_t dir_size;
    uint64_t dir_start;
    uint64_t data_start;
    uint64_t bytes_per_block;
    uint64_t sectors_per_block;
    uint64_t entries_per_block;

    struct path_result_table path_cache;
    struct entry_t *dir_table;
    uint64_t *fat;
}  dhfs;

static struct dhfs_handle_t handles[MAX_HANDLES];

static char *internal_strchrnul(const char *s, char c) {
    while (*s) {
        if ((*s++) == c)
            break;
    }
    return (char *) s;
}

static void dhfs_debug(const char *fmt, ...) {
#ifdef dhfs_DEBUG
    va_list args;
    va_start(args, fmt);
    vfprintf(stdout, fmt, args);
    va_end(args);
#else
    (void)fmt;
#endif
}

static void cleanup_fuse() {
    fuse_unmount(dhfs.mountpoint, dhfs.chan);
    fuse_remove_signal_handlers(dhfs.session);
}

static inline int dhfs_fseek(FILE *file, long loc, int mode) {
    return fseek(file, dhfs.part_offset + loc, mode);
}

static inline uint16_t rd_word(long loc) {
    uint16_t x = 0;
    dhfs_fseek(dhfs.image, loc, SEEK_SET);
    int ret = fread(&x, 2, 1, dhfs.image);
    if (ret != 1)
        fprintf(stderr, "error reading word!\n");
    return x;
}

static inline uint64_t rd_qword(long loc) {
    uint64_t x = 0;
    dhfs_fseek(dhfs.image, loc, SEEK_SET);
    int ret = fread(&x, 8, 1, dhfs.image);
    if (ret != 1)
        fprintf(stderr, "error reading qword!\n");
    return x;
}

static void rd_entry(struct entry_t *entry, uint64_t pos) {
    memcpy(entry, dhfs.dir_table + pos, sizeof(struct entry_t));
}

static void wr_entry(struct entry_t *entry, uint64_t pos) {
    memcpy(dhfs.dir_table + pos, entry, sizeof(struct entry_t));
}

static inline uint64_t get_time() {
    struct timeval time = {0};
    gettimeofday(&time, NULL);
    return time.tv_sec;
}

static int update_mtime(struct path_result_t *path_res) {
    uint64_t time = get_time();
    path_res->target.mtime = time;
    wr_entry(&path_res->target, path_res->target_entry);
    return 0;
}

static int detect_cycle(struct path_result_t* list) {
#ifdef dhfs_DEBUG
    struct path_result_t *slow_p = list, *fast_p = list;
    while (slow_p && fast_p && fast_p->next) {
        slow_p = slow_p->next;
        fast_p = fast_p->next->next;
        if (slow_p == fast_p) {
            dhfs_debug("Found loop, slow_p is %s, fast_p is %s\n",
                    slow_p->path, fast_p->path);
            return 1;
        }
    }
    return 0;
#else
    (void)list;
    return 0;
#endif
}

static inline uint64_t hash_str(const char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++))
        hash = ((hash << 5) + hash) + c;
    return hash;
}

static struct path_result_table init_table(uint64_t size) {
    struct path_result_table table = {0};
    table.table = calloc(sizeof(struct path_result_t*), size);
    table.size = size;
    return table;
}

static void rehash_path(const char *path, const char *new) {
    uint64_t hash = hash_str(path);
    uint64_t offset = hash % dhfs.path_cache.size;

    struct path_result_t *element = dhfs.path_cache.table[offset];
    struct path_result_t *prev = NULL;
    for (; element; prev = element, element = element->next) {
        if (!strcmp(element->path, new)) {
            if (!prev) {
                dhfs.path_cache.table[offset] = element->next;
            } else {
                prev->next = element->next;
            }
            break;
        }
    }
    element->next = NULL;
    if(detect_cycle(dhfs.path_cache.table[offset]))
        dhfs_debug("detected cycle in rehash_path\n");

    uint64_t new_hash = hash_str(element->path);
    uint64_t new_offset = new_hash % dhfs.path_cache.size;
    if (!dhfs.path_cache.table[new_offset]) {
        dhfs.path_cache.table[new_offset] = element;
    } else {
        struct path_result_t *it = dhfs.path_cache.table[new_offset];
        for (; it->next; it = it->next);
        it->next = element;
    }
    if(detect_cycle(dhfs.path_cache.table[new_offset]))
        dhfs_debug("detected cycle in rehash_path\n");
}

static void remove_cached_path(const char *path) {
    uint64_t hash = hash_str(path);
    uint64_t offset = hash % dhfs.path_cache.size;

    struct path_result_t *element = dhfs.path_cache.table[offset];
    struct path_result_t *prev = NULL;
    for (; element; prev = element, element = element->next) {
        if (!strcmp(element->path, path)) {
            if (!prev) {
                dhfs.path_cache.table[offset] = element->next;
                free(element);
            } else {
                prev->next = element->next;
                free(element);
            }
            break;
        }
    }
    if(detect_cycle(dhfs.path_cache.table[offset]))
        dhfs_debug("detected cycle in remove_cached_path with path %s\n",
                path);
}

static void insert_cached_path(struct path_result_t *path_res,
        struct path_result_table *table) {
    uint64_t hash = hash_str(path_res->path);
    uint64_t offset = hash % table->size;

    if (!table->table[offset]) {
        table->table[offset] = path_res;
    } else {
        struct path_result_t *element = table->table[offset];
        for (; element->next; element = element->next);
        element->next = path_res;
    }
    if(detect_cycle(table->table[offset]))
        dhfs_debug("detected cycle in insert_cached_path with path %s\n",
                path_res->path);

    table->num_elements++;
}

static void cache_path(struct path_result_t *path_res) {
    double load_factor = (dhfs.path_cache.num_elements + 1) /
        dhfs.path_cache.size;
    if (load_factor > 0.75) {
        dhfs_debug("rehashing table!\n");
        /* rehash */
        struct path_result_table new_table = init_table(dhfs.path_cache.size
                * 2);
        for (int i = 0; i < dhfs.path_cache.size; i++) {
            struct path_result_t *element = dhfs.path_cache.table[i];
            while (element) {
                struct path_result_t *next = element->next;
                element->next = NULL;
                insert_cached_path(element, &new_table);
                element = next;
            }
        }
        free(dhfs.path_cache.table);
        dhfs.path_cache = new_table;
    }

    insert_cached_path(path_res, &dhfs.path_cache);
}

static struct path_result_t *get_cached_path(const char *path) {
    uint64_t hash = hash_str(path);
    uint64_t offset = hash % dhfs.path_cache.size;
    struct path_result_t *result = dhfs.path_cache.table[offset];
    for (; result; result = result->next) {
        if (!strcmp(result->path, path)) return result;
    }
    return NULL;
}

static void *dhfs_init(struct fuse_conn_info *conn) {
    (void) conn;

    memset(&handles, 0, sizeof(handles));
    dhfs.image = fopen(dhfs.image_path, "r+");
    if (!dhfs.image) {
        fprintf(stderr, "Error opening dhfs image %s!\n", dhfs.image_path);
        cleanup_fuse();
        exit(1);
    }

    if (dhfs.mbr) {
        struct part p;
        mbr_get_part(&p, dhfs.image, dhfs.partition);
        dhfs.part_offset = p.first_sect * 512;
        dhfs.image_size  = p.sect_count * 512;
    } else if (dhfs.gpt) {
        struct part p;
        gpt_get_part(&p, dhfs.image, dhfs.partition);
        dhfs.part_offset = p.first_sect * 512;
        dhfs.image_size  = p.sect_count * 512;
    } else {
        dhfs.part_offset = 0;
        fseek(dhfs.image, 0L, SEEK_END);
        dhfs.image_size = (uint64_t)ftell(dhfs.image);
        dhfs_fseek(dhfs.image, 0L, SEEK_SET);
    }
    dhfs_debug("dhfs image size: %lu\n", dhfs.image_size);

    char signature[8] = {0};
    dhfs_fseek(dhfs.image, 4, SEEK_SET);
    int ret = fread(signature, 8, 1, dhfs.image);
    if (ret != 1) {
        fprintf(stderr, "error reading signature!\n");
        cleanup_fuse();
        fclose(dhfs.image);
        exit(1);
    }

    if (strncmp(signature, "_DH_FS_", 8)) {
        fprintf(stderr, "DragonHeartFS signature missing!\n");
        cleanup_fuse();
        fclose(dhfs.image);
        exit(1);
    }

    dhfs.fat_start = RESERVED_BLOCKS;
    dhfs.bytes_per_block = rd_qword(28);
    dhfs_debug("dhfs block size: %lu\n", dhfs.bytes_per_block);
    if (dhfs.image_size % dhfs.bytes_per_block) {
        fprintf(stderr, "image is not block aligned!\n");
        cleanup_fuse();
        fclose(dhfs.image);
        exit(1);
    }

    dhfs.blocks = dhfs.image_size / dhfs.bytes_per_block;
    dhfs_debug("dhfs block count: %lu\n", dhfs.blocks);
    uint64_t declared_blocks = rd_qword(12);
    if (declared_blocks != dhfs.blocks) {
        fprintf(stderr, "warning: declared block count mismatch, declared: "
                "%lu, real: %lu\n", declared_blocks, dhfs.blocks);
    }

    dhfs.sectors_per_block = dhfs.bytes_per_block / BYTES_PER_SECT;
    dhfs.entries_per_block = dhfs.sectors_per_block * ENTRIES_PER_SECT;

    dhfs.fat_size = (dhfs.blocks * sizeof(uint64_t)) / dhfs.bytes_per_block;
    if ((dhfs.blocks * sizeof(uint64_t)) % dhfs.bytes_per_block) {
        dhfs.fat_size++;
    }
    dhfs_debug("dhfs allocation table size: %lu\n", dhfs.fat_size);
    dhfs_debug("dhfs allocation table start: %lu\n", dhfs.fat_start);

    dhfs.dir_size = rd_qword(20);
    dhfs_debug("dhfs dir size: %lu\n", dhfs.dir_size);
    dhfs.dir_start = dhfs.fat_start + dhfs.fat_size;
    dhfs_debug("dhfs dir start: %lu\n", dhfs.dir_start);

    dhfs.data_start = RESERVED_BLOCKS + dhfs.fat_size + dhfs.dir_size;
    dhfs_debug("dhfs data start: %lu\n", dhfs.data_start);
    dhfs_debug("dhfs usable blocks: %lu\n", dhfs.blocks -
            dhfs.data_start);

    dhfs_debug("image is %s\n", rd_word(510) == 0xAA55 ? "bootable" :
            "NOT bootable");

    dhfs.path_cache = init_table(1024);
    dhfs.dir_table = malloc(dhfs.dir_size * dhfs.bytes_per_block);
    if (!dhfs.dir_table) {
        fprintf(stderr, "error allocating dir_table!\n");
        cleanup_fuse();
        fclose(dhfs.image);
        exit(1);
    }
    dhfs_fseek(dhfs.image, dhfs.dir_start * dhfs.bytes_per_block, SEEK_SET);
    ret = fread(dhfs.dir_table, sizeof(char), dhfs.dir_size *
            dhfs.bytes_per_block, dhfs.image);
    if (ret != (dhfs.dir_size * dhfs.bytes_per_block)) {
        fprintf(stderr, "error reading dir_table!\n");
        cleanup_fuse();
        fclose(dhfs.image);
        free(dhfs.dir_table);
        exit(1);
    }

    dhfs.fat = malloc(dhfs.fat_size * dhfs.bytes_per_block);
    if (!dhfs.fat) {
        fprintf(stderr, "error allocating allocation table!\n");
        cleanup_fuse();
        fclose(dhfs.image);
        free(dhfs.dir_table);
        exit(1);
    }
    dhfs_fseek(dhfs.image, dhfs.fat_start * dhfs.bytes_per_block, SEEK_SET);
    ret = fread(dhfs.fat, sizeof(char), dhfs.fat_size * dhfs.bytes_per_block,
            dhfs.image);
    if (ret != (dhfs.fat_size * dhfs.bytes_per_block)) {
        fprintf(stderr, "error reading allocation table!\n");
        cleanup_fuse();
        fclose(dhfs.image);
        free(dhfs.dir_table);
        free(dhfs.fat);
        exit(1);
    }

    return NULL;
}

static void dhfs_destroy(void *data) {
    (void) data;
    fprintf(stderr, "cleaning up!\n");
    dhfs_fseek(dhfs.image, dhfs.dir_start * dhfs.bytes_per_block, SEEK_SET);
    fwrite(dhfs.dir_table, sizeof(char), dhfs.dir_size * dhfs.bytes_per_block,
            dhfs.image);
    free(dhfs.dir_table);

    dhfs_fseek(dhfs.image, dhfs.fat_start * dhfs.bytes_per_block, SEEK_SET);
    fwrite(dhfs.fat, sizeof(char), dhfs.dir_size * dhfs.bytes_per_block,
            dhfs.image);
    free(dhfs.fat);
    fclose(dhfs.image);
}

static int is_dir_empty(uint64_t id) {
    for (uint64_t i = 0; i < (dhfs.dir_size * dhfs.entries_per_block); i++) {
        struct entry_t *entry = &dhfs.dir_table[i];
        if (!entry->parent_id) return 1;
        if (entry->parent_id == id) return 0;
    }

    return 1;
}

static uint64_t search(const char *name, uint64_t parent) {
    for (uint64_t i = 0; i < (dhfs.dir_size * dhfs.entries_per_block); i++) {
        struct entry_t *entry = &dhfs.dir_table[i];
        if (!entry->parent_id) return SEARCH_FAILURE;
        if ((entry->parent_id == parent) && (!strcmp(entry->name, name))) {
            return i;
        }
    }
    return SEARCH_FAILURE;
}

static struct path_result_t *resolve_path(const char *path) {
    struct path_result_t *path_result = get_cached_path(path);
    if (path_result) {
        dhfs_debug("found cached path %s\n", path);
        path_result->failure = 0;
        return path_result;
    }

    path_result = malloc(sizeof(struct path_result_t));
    path_result->next = NULL;
    strcpy(path_result->path, path);
    path_result->type = DIRECTORY_TYPE;

    path_result->parent.payload = ROOT_ID;

    if (!strncmp(path, "/\0", 2)) {
        strncpy(path_result->name, "/\0", 2);
        path_result->failure = 0;
        path_result->target_entry = -1;
        path_result->target.parent_id = -1;
        path_result->target.type = DIRECTORY_TYPE;
        strncpy(path_result->target.name, "/\0", 2);
        path_result->target.payload = ROOT_ID;
        return path_result;
    }

    if (*path == '/') path++;

    struct entry_t entry;
    path_result->target.payload = ROOT_ID;
    do {
        const char *seg = path;
        path = internal_strchrnul(path, '/');
        size_t seg_length = path - seg;
        if (seg[seg_length - 1] == '/')
            seg_length--;
        char *seg_buf = malloc(seg_length + 1);
        strncpy(seg_buf, seg, seg_length);
        seg_buf[seg_length] = '\0';
        dhfs_debug("resolve_path(): looking for %s\n", seg_buf);

        uint64_t search_res = search(seg_buf, path_result->target.payload);
        if (search_res == SEARCH_FAILURE) {
            dhfs_debug("resolve_path(): search failure for %s\n", seg_buf);
            strcpy(path_result->name, seg_buf);
            path_result->parent = path_result->target;
            path_result->failure = 1;
            free(seg_buf);
            return path_result;
        }

        rd_entry(&entry, search_res);
        path_result->parent = path_result->target;
        path_result->target = entry;
        path_result->target_entry = search_res;
        dhfs_debug("resolve_path(): found %s\n", seg_buf);
        free(seg_buf);
    } while (*path);

    strcpy(path_result->name, entry.name);
    path_result->type = path_result->target.type;
    path_result->failure = 0;
    cache_path(path_result);
    return path_result;
}

static int get_handle() {
    for (int i = 0; i < MAX_HANDLES; i++) {
        if (!handles[i].occupied) return i;
    }
    return -1;
}

static int dhfs_open(const char *file_path, struct fuse_file_info *file_info) {
    dhfs_debug("opening file %s\n", file_path);
    struct path_result_t *path_result = resolve_path(file_path);
    if (path_result->failure) return -ENOENT;
    if (path_result->target.type == DIRECTORY_TYPE) return -EISDIR;

    int handle_num = get_handle();
    if (handle_num < 0) return -ENOMEM;
    file_info->fh = handle_num;

    struct dhfs_handle_t *handle = &handles[file_info->fh];
    handle->path_res = path_result;
    handle->occupied = 1;

    handle->alloc_map = malloc(sizeof(uint64_t));
    handle->alloc_map[0] = path_result->target.payload;
    uint64_t i = 1;
    for (i = 1; handle->alloc_map[i - 1] != END_OF_CHAIN; i++) {
        handle->alloc_map = realloc(handle->alloc_map,
                sizeof(uint64_t) * (i + 1));
        handle->alloc_map[i] = dhfs.fat[handle->alloc_map[i - 1]];
    }

    handle->total_blocks = i - 1;

    return 0;
}

static int dhfs_opendir(const char *dir_path,
        struct fuse_file_info *file_info) {
    dhfs_debug("opening dir %s\n", dir_path);
    struct path_result_t *path_result = resolve_path(dir_path);
    if (path_result->failure) {
        return -ENOENT;
    }

    if (path_result->target.type == FILE_TYPE) {
        return -ENOTDIR;
    }

    int handle = get_handle();
    if (handle < 0) return -ENOMEM;
    file_info->fh = handle;

    handles[handle].path_res = path_result;
    handles[handle].occupied = 1;
    return 0;
}

static int dhfs_fgetattr(const char *path, struct stat *stat,
        struct fuse_file_info *file_info) {
    dhfs_debug("fgetattr() on %s\n", path);
    if (file_info->fh >= MAX_HANDLES) return -EBADF;
    if (!handles[file_info->fh].occupied) return -EBADF;
    struct dhfs_handle_t *handle = &handles[file_info->fh];
    struct path_result_t *path_result = handle->path_res;

    stat->st_ino = path_result->target_entry + 1;
    stat->st_nlink = 1;
    stat->st_rdev = 0;
    stat->st_size = path_result->target.size;
    stat->st_blksize = 512;
    stat->st_blocks = (stat->st_size + 512 - 1) / 512;
    stat->st_atim.tv_sec = path_result->target.atime;
    stat->st_atim.tv_nsec = 0;
    stat->st_mtim.tv_sec = path_result->target.mtime;
    stat->st_mtim.tv_nsec = 0;

    stat->st_mode = 0;
    switch (path_result->target.type) {
        case DIRECTORY_TYPE:
            stat->st_mode |= S_IFDIR;
            break;
        case FILE_TYPE:
            stat->st_mode |= S_IFREG;
            break;
    }
    stat->st_mode |= S_IRWXU;
    stat->st_mode |= S_IRWXO;
    return 0;
}

static int dhfs_getattr(const char *path, struct stat *stat) {
    dhfs_debug("getattr() on %s\n", path);

    struct path_result_t *path_result = resolve_path(path);
    if (path_result->failure) {
        return -ENOENT;
    }

    stat->st_ino = path_result->target_entry + 1;
    stat->st_nlink = 1;
    stat->st_rdev = 0;
    stat->st_size = path_result->target.size;
    stat->st_blksize = 512;
    stat->st_blocks = (stat->st_size + 512 - 1) / 512;
    stat->st_atim.tv_sec = path_result->target.atime;
    stat->st_atim.tv_nsec = 0;
    stat->st_mtim.tv_sec = path_result->target.mtime;
    stat->st_mtim.tv_nsec = 0;

    stat->st_mode = 0;
    switch (path_result->target.type) {
        case DIRECTORY_TYPE:
            stat->st_mode |= S_IFDIR;
            break;
        case FILE_TYPE:
            stat->st_mode |= S_IFREG;
            break;
    }

    return 0;
}

static int dhfs_readdir(const char *path, void *buf, fuse_fill_dir_t fill,
        off_t offset, struct fuse_file_info *file_info) {
    dhfs_debug("readdir() on %s and offset %lu\n", path, offset);

    struct dhfs_handle_t *handle = &handles[file_info->fh];
    if (!handle->occupied || file_info->fh >= MAX_HANDLES) return -EBADF;
    if (handle->path_res->target.type != DIRECTORY_TYPE)
        return -ENOTDIR;

    uint64_t dir_id = handle->path_res->target.payload;
    off_t i = offset;
    for (;; i++) {
        if (i >= (dhfs.dir_size * dhfs.entries_per_block)) return 0;
        struct entry_t *entry = &dhfs.dir_table[i];
        if (!entry->parent_id) return 0;
        if (entry->parent_id == dir_id) {
            if(fill(buf, entry->name, NULL, i + 1)) return 0;
        }
    }
    return 0;
}

static int dhfs_release(const char *path,
        struct fuse_file_info *file_info) {
    if (file_info->fh >= MAX_HANDLES) return -EBADF;
    if (!handles[file_info->fh].occupied) return -EBADF;
    if (handles[file_info->fh].path_res->type != FILE_TYPE) return -EISDIR;

    dhfs_debug("released handle for %s\n", path);
    handles[file_info->fh].occupied = 0;
    free(handles[file_info->fh].alloc_map);
    return 0;
}

static int dhfs_releasedir(const char *path,
        struct fuse_file_info *file_info) {
    if (file_info->fh >= MAX_HANDLES) return -EBADF;
    if (!handles[file_info->fh].occupied) return -EBADF;
    if (handles[file_info->fh].path_res->type != DIRECTORY_TYPE)
        return -EISDIR;

    handles[file_info->fh].occupied = 0;
    return 0;
}

static int dhfs_read(const char *path, char *buf, size_t to_read,
        off_t offset, struct fuse_file_info *file_info) {
    dhfs_debug("dhfs_read() on %s, %lu\n", path, to_read);
    if (file_info->fh >= MAX_HANDLES) return -EBADF;
    if (!handles[file_info->fh].occupied) return -EBADF;
    if (handles[file_info->fh].path_res->type != FILE_TYPE) return -EISDIR;

    struct dhfs_handle_t *handle = &handles[file_info->fh];
    if ((offset + to_read) >= handle->path_res->target.size)
        to_read = handle->path_res->target.size - offset;

    uint64_t progress = 0;
    while (progress < to_read) {
        uint64_t block = (offset + progress) / dhfs.bytes_per_block;
        uint64_t loc = handle->alloc_map[block] * dhfs.bytes_per_block;

        uint64_t chunk = to_read - progress;
        uint64_t disk_offset = (offset + progress) % dhfs.bytes_per_block;
        if (chunk > dhfs.bytes_per_block - disk_offset)
            chunk = dhfs.bytes_per_block - disk_offset;

        dhfs_fseek(dhfs.image, loc + disk_offset, SEEK_SET);
        int ret = fread(buf + progress, 1, chunk, dhfs.image);
        if (ret != chunk)
            return -EIO;
        progress += chunk;
    }

    return to_read;
}

//TODO check if we run out of space..
static uint64_t allocate_new_block(uint64_t prev_block) {
    uint64_t i = 0;
    for (;; i++) {
        uint64_t block = dhfs.fat[i];
        if (!block) {
            dhfs.fat[i] = END_OF_CHAIN;
            if (prev_block) {
                dhfs.fat[prev_block] = i;
            }
            break;
        }
    }
    return i;
}

static uint64_t get_block_pos(struct dhfs_handle_t *handle, uint64_t block) {
    if (block >= handle->total_blocks) {
        uint64_t new_block_count = block + 1;
        handle->alloc_map = realloc(handle->alloc_map,
                new_block_count * sizeof(uint64_t));
        for (uint64_t i = handle->total_blocks; i < new_block_count; i++) {
            uint64_t new_block = 0;
            if (!i) {
                new_block = allocate_new_block(0);
                handle->path_res->target.payload = new_block;
            } else {
                new_block = allocate_new_block(handle->alloc_map[i - 1]);
            }
            handle->alloc_map[i] = new_block;
        }
        handle->total_blocks = new_block_count;
        wr_entry(&handle->path_res->target,
                handle->path_res->target_entry);
    }
    return handle->alloc_map[block];
}

static int dhfs_write(const char *path, const char *buf, size_t to_write,
        off_t offset, struct fuse_file_info *file_info) {
    dhfs_debug("dhfs_write() on %s\n", path);
    if (file_info->fh >= MAX_HANDLES) return -EBADF;
    if (!handles[file_info->fh].occupied) return -EBADF;
    if (handles[file_info->fh].path_res->type != FILE_TYPE) return -EISDIR;

    struct dhfs_handle_t *handle = &handles[file_info->fh];
    int ret = update_mtime(handle->path_res);
    if (ret) return ret;

    if ((offset + to_write) > handle->path_res->target.size) {
        handle->path_res->target.size = offset + to_write;
        wr_entry(&handle->path_res->target,
                handle->path_res->target_entry);
    }

    uint64_t progress = 0;
    while (progress < to_write) {
        uint64_t block = (offset + progress) / dhfs.bytes_per_block;
        uint64_t loc = get_block_pos(handle, block) * dhfs.bytes_per_block;

        uint64_t chunk = to_write - progress;
        uint64_t buf_offset = (offset + progress) % dhfs.bytes_per_block;
        if (chunk > dhfs.bytes_per_block - buf_offset)
            chunk = dhfs.bytes_per_block - buf_offset;

        dhfs_fseek(dhfs.image, loc + buf_offset, SEEK_SET);
        ret = fwrite(buf + progress, 1, chunk, dhfs.image);
        if (ret != chunk)
            return -EIO;
        progress += chunk;
    }

    return to_write;
}

static uint64_t find_free_entry() {
    uint64_t i = 0;
    struct entry_t entry;
    for (; ; i++) {
        if (i >= (dhfs.dir_size * dhfs.entries_per_block))
            return SEARCH_FAILURE;
        entry = dhfs.dir_table[i];
        if (!entry.parent_id) break;
        if (entry.parent_id == DELETED_ENTRY) break;
    }

    return i;
}

static int dhfs_create(const char *path, mode_t mode,
        struct fuse_file_info *file_info) {
    dhfs_debug("dhfs_create() on %s\n", path);
    struct path_result_t *path_res = resolve_path(path);
    if (!path_res->failure)
        return -EEXIST;

    struct entry_t entry = {0};
    entry.parent_id = path_res->parent.payload;
    entry.type = FILE_TYPE;
    strcpy(entry.name, path_res->name);
    entry.payload = END_OF_CHAIN;

    uint64_t new_entry = find_free_entry();
    if (new_entry == SEARCH_FAILURE) return -EIO;
    wr_entry(&entry, new_entry);

    path_res->target = entry;
    path_res->target_entry = new_entry;
    path_res->type = FILE_TYPE;
    path_res->failure = 0;
    cache_path(path_res);

    int handle_num = get_handle();
    if (handle_num < 0) return -ENOMEM;
    file_info->fh = handle_num;

    struct dhfs_handle_t *handle = &handles[file_info->fh];
    handle->path_res = path_res;
    handle->occupied = 1;
    handle->alloc_map = NULL;

    handle->total_blocks = 0;
    return 0;
}

static uint64_t find_free_dir_id() {
    uint64_t id = 1;
    uint64_t i = 0;
    struct entry_t entry = {0};

    for (; ; i++) {
        if (i >= (dhfs.dir_size * dhfs.entries_per_block))
            return SEARCH_FAILURE;
        entry = dhfs.dir_table[i];
        if (!entry.parent_id) break;
        if (entry.parent_id == DELETED_ENTRY) continue;
        if ((entry.type == 1) && (entry.payload == id))
            id = (entry.payload + 1);
    }

    return id;
}

static int dhfs_mkdir(const char *path, mode_t mode) {
    dhfs_debug("dhfs_mkdir() on %s\n", path);
    struct path_result_t *path_res = resolve_path(path);
    if (!path_res->failure)
        return -EEXIST;

    uint64_t new_entry = find_free_entry();
    if (new_entry == SEARCH_FAILURE) return -EIO;
    uint64_t new_dir_id = find_free_dir_id();
    if (new_dir_id == SEARCH_FAILURE) return -EIO;

    struct entry_t entry = {0};
    entry.parent_id = path_res->parent.payload;
    entry.type = DIRECTORY_TYPE;
    strcpy(entry.name, path_res->name);
    entry.payload = new_dir_id;
    entry.atime = entry.mtime = get_time();

    wr_entry(&entry, new_entry);

    path_res->target_entry = new_entry;
    path_res->target = entry;
    path_res->failure = 0;
    path_res->type = DIRECTORY_TYPE;
    return 0;
}

static int dhfs_unlink(const char *path) {
    struct path_result_t *path_res = resolve_path(path);
    if (path_res->failure)
        return -ENOENT;
    if (path_res->type == DIRECTORY_TYPE)
        return -EISDIR;

    uint64_t empty = 0;
    uint64_t block = path_res->target.payload;
    if (block != END_OF_CHAIN) {
        for (;;) {
            uint64_t next_block = dhfs.fat[block];
            dhfs.fat[block] = empty;
            if (next_block == END_OF_CHAIN)
                break;
            block = next_block;
        }
    }

    struct entry_t deleted_entry = {0};
    deleted_entry.parent_id = DELETED_ENTRY;
    wr_entry(&deleted_entry, path_res->target_entry);
    remove_cached_path(path);
    return 0;
}

static int dhfs_rmdir(const char *path) {
    struct path_result_t *path_res = resolve_path(path);
    if (path_res->failure)
        return -ENOENT;
    if (path_res->type == FILE_TYPE)
        return -ENOTDIR;

    int ret = is_dir_empty(path_res->target.payload);
    if (ret < 0) return ret;
    if (!ret) return -ENOTEMPTY;

    struct entry_t deleted_entry = {0};
    deleted_entry.parent_id = DELETED_ENTRY;
    wr_entry(&deleted_entry, path_res->target_entry);
    remove_cached_path(path);
    return 0;
}

static int dhfs_utimens(const char *path, const struct timespec tv[2]) {
    dhfs_debug("dhfs_utimens() on %s\n", path);
    struct path_result_t *path_res = resolve_path(path);

    path_res->target.atime = tv[0].tv_sec;
    path_res->target.mtime = tv[1].tv_sec;

    wr_entry(&path_res->target, path_res->target_entry);
    return 0;
}

//TODO: free the blocks too
static int dhfs_truncate(const char *path, off_t size) {
    dhfs_debug("dhfs_truncate() on %s, size %lu\n", path, size);
    struct path_result_t *path_res = resolve_path(path);
    path_res->target.size = size;
    wr_entry(&path_res->target, path_res->target_entry);
    return 0;
}

static int dhfs_ftruncate(const char *path, off_t size,
        struct fuse_file_info *file_info) {
    dhfs_debug("dhfs_ftruncate() on %s, size %lu\n", path, size);
    if (file_info->fh >= MAX_HANDLES) return -EBADF;
    if (!handles[file_info->fh].occupied) return -EBADF;
    if (handles[file_info->fh].path_res->type != FILE_TYPE) return -EISDIR;

    struct dhfs_handle_t *handle = &handles[file_info->fh];
    handle->path_res->target.size = size;
    wr_entry(&handle->path_res->target,
            handle->path_res->target_entry);
    return 0;
}

static int dhfs_rename(const char *path, const char *new) {
    dhfs_debug("dhfs_rename() on %s, %s\n", path, new);
    struct path_result_t *path_res = resolve_path(path);
    if (path_res->failure)
        return -ENOENT;

    dhfs_unlink(new);

    const char *new_name = strrchr(new, '/');
    if (!new_name)
        new_name = new;
    else
        new_name++;

    strcpy(path_res->target.name, new_name);
    wr_entry(&path_res->target, path_res->target_entry);

    strcpy(path_res->name, new_name);
    strcpy(path_res->path, new);
    rehash_path(path, new);
    return 0;
}

static struct fuse_operations operations = {
    .init = dhfs_init,
    .destroy = dhfs_destroy,
    .open = dhfs_open,
    .opendir = dhfs_opendir,
    .fgetattr = dhfs_fgetattr,
    .getattr = dhfs_getattr,
    .readdir = dhfs_readdir,
    .release = dhfs_release,
    .releasedir = dhfs_releasedir,
    .read = dhfs_read,
    .write = dhfs_write,
    .create = dhfs_create,
    .unlink = dhfs_unlink,
    .utimens = dhfs_utimens,
    .truncate = dhfs_truncate,
    .ftruncate = dhfs_ftruncate,
    .mkdir = dhfs_mkdir,
    .rmdir = dhfs_rmdir,
    .rename = dhfs_rename,
};

static struct options {
    int show_help;
    int debug;
    int mbr;
    int gpt;
    int partition;
} options;

#define OPTION(t, p)    \
    { t, offsetof(struct options, p), 1 }
static const struct fuse_opt option_spec[] = {
    OPTION("-h", show_help),
    OPTION("--help", show_help),
    OPTION("-d", debug),
    OPTION("--mbr", mbr),
    OPTION("--gpt", gpt),
    OPTION("-p %i", partition),
    FUSE_OPT_END
};

static int option_cb(void *data, const char *arg, int key,
        struct fuse_args *outargs) {
    (void) outargs;
    (void) data;
    switch (key) {
        case FUSE_OPT_KEY_NONOPT:
            if (!dhfs.image_path) {
                dhfs.image_path = strdup(arg);
                return 0;
            } else if (!dhfs.mountpoint) {
                dhfs.mountpoint = strdup(arg);
                return 0;
            }
    }
    return 1;
}

static void show_help(const char *program_name) {
    printf("usage: %s [options] <dhfs image> <mountpoint>\n", program_name);
}

int main(int argc, char **argv) {
    dhfs.image_path = dhfs.mountpoint = 0;
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    if (fuse_opt_parse(&args, &options, option_spec, option_cb)) {
        fprintf(stderr, "Error reading command line options\n");
        return 1;
    }

    if (options.show_help) {
        show_help(argv[0]);
        assert(fuse_opt_add_arg(&args, "--help") == 0);
        args.argv[0][0] = '\0';
    }

    if (!dhfs.image_path || !dhfs.mountpoint) {
        fprintf(stderr, "Please specify dhfs image and mountpoint!\n");
        fuse_opt_free_args(&args);
        return 1;
    }

    char *real_mountpoint = realpath(dhfs.mountpoint, NULL);
    char *real_image_path = realpath(dhfs.image_path, NULL);
    free(dhfs.mountpoint);
    free(dhfs.image_path);
    dhfs.mountpoint = real_mountpoint;
    dhfs.image_path = real_image_path;
    dhfs.mbr = options.mbr;
    dhfs.gpt = options.gpt;
    dhfs.partition = options.partition;

    struct fuse_chan *chan = fuse_mount(dhfs.mountpoint, &args);
    if (!chan) {
        fuse_opt_free_args(&args);
        return 1;
    }

    struct fuse *fuse = fuse_new(chan, &args, &operations,
            sizeof(struct fuse_operations), NULL);
    if (!fuse) {
        fprintf(stderr, "Error initializing fuse!\n");
        fuse_opt_free_args(&args);
        return 1;
    }

    struct fuse_session *session = fuse_get_session(fuse);
    int ret = fuse_set_signal_handlers(session);
    if (ret) {
        fuse_destroy(fuse);
        fuse_opt_free_args(&args);
        return 1;
    }

    dhfs.chan = chan;
    dhfs.session = session;

    fuse_daemonize(options.debug);
    ret = fuse_loop(fuse);

    cleanup_fuse();
    fuse_destroy(fuse);
    fuse_opt_free_args(&args);
    return ret;
}
