/*
 *  Copyright (C) 2021 CS416 Rutgers CS
 *	Tiny File System
 *	File:	tfs.c
 *
 */

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/time.h>
#include <libgen.h>
#include <limits.h>
#include <stdbool.h>

#include "block.h"
#include "tfs.h"

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

#define ROOT_INO 	0
#define TYPE_DIR 	0
#define TYPE_FILE 	1


/********** Local Function Definitions **********/

int get_avail_ino();
int get_avail_blkno();
void clear_bmap_ino(int i);
void clear_bmap_blkno(int i);

void inode_init(inode_t *inode, uint16_t ino, uint32_t type);
int readi(uint16_t ino, inode_t *inode);
int writei(uint16_t ino, inode_t *inode);

void dirent_init(dirent_t *dirent, uint16_t ino, const char *name, size_t name_len);
int dir_find(uint16_t ino, const char *fname, size_t name_len, dirent_t *dirent_p);
int dir_add(inode_t *dir_inode, uint16_t f_ino, const char *fname, size_t name_len);
int dir_remove(inode_t *dir_inode, const char *fname, size_t name_len);
int get_node_by_path(const char *path, uint16_t ino, inode_t *inode);

void parse_name(const char *path, char *parent, char *target);
int check_and_alloc(inode_t *inode, int i);


/************** Static Variables **************/

static superblock_t superblock;
static char diskfile_path[PATH_MAX];
static bool flag;


/************** Bitmap Functions **************/

/* 
 * Find first available bit (bit = 0) in inode bitmap and returns
 * its index. Returns -1 if no bits are available. 
 */
int get_avail_ino() {
	/* Read bitmap from disk into stack */
	char block[BLOCK_SIZE];
	bio_read(superblock.i_bitmap_blk, block);

	/* Loop through each byte in bitmap*/
	for (int i=0; i < MAX_INUM/8; i++) {
		/* Check if all bits are set to save comparisons */
		if (block[i] == ~0)
			continue;

		/* Loop through by bit */
		for (int j=0; j < 8; j++) {
			int index = i*8+j;
			if (get_bitmap(block, index) == 0) {
				set_bitmap(block, index);
				bio_write(superblock.i_bitmap_blk, block);
				return index;
			}
		}
	}
	return -1;  /* No free bit found */
}

/* 
 * Same as get_avail_ino() but for data block bitmap.
 */
int get_avail_blkno() {
	char block[BLOCK_SIZE];
	bio_read(superblock.d_bitmap_blk, block);
	for (int i=0; i < MAX_DNUM/8; i++) {
		if (block[i] == ~0)
			continue;
		for (int j=0; j < 8; j++) {
			int index = i*8+j;
			if (get_bitmap(block, index) == 0) {
				set_bitmap(block, index);
				bio_write(superblock.d_bitmap_blk, block);
				return superblock.d_start_blk+index;
			}
		}
	}
	return -1;
}

/* 
 * Sets bit at i-th index in inode bitmap and stores changes in disk.
 */
void clear_bmap_ino(int i) {
	char block[BLOCK_SIZE];
	bio_read(superblock.i_bitmap_blk, block);
	unset_bitmap(block, i);
	bio_write(superblock.i_bitmap_blk, block);
}

/* 
 * Sets bit at i-th index in data block bitmap and stores changes in disk.
 */
void clear_bmap_blkno(int i) {
	char block[BLOCK_SIZE];
	bio_read(superblock.d_bitmap_blk, block);
	unset_bitmap(block, i-superblock.d_start_blk);
	bio_write(superblock.d_bitmap_blk, block);
}


/************** INode Functions **************/

/* 
 * Stores default values for inode at *inode and invalidates all direct pointers.
 * Note this function does not write to disk.
 */
void inode_init(inode_t *inode, uint16_t ino, uint32_t type) {
	inode->ino = ino;
	inode->type = type;
	inode->valid = 1;
	inode->size = 0;
	inode->link = 0;
	for (int i=0; i < 16; i++) {
		inode->direct_ptr[i] = -1;
	}
}

/* 
 * Reads inode from disk and stores data in *inode. Assuming ino is valid inode
 * number.
 */
int readi(uint16_t ino, inode_t *inode) {
	int block_num = superblock.i_start_blk + ino / (BLOCK_SIZE/sizeof(inode_t));
	size_t offset = sizeof(inode_t) * (ino % (BLOCK_SIZE/sizeof(inode_t)));

	char block[BLOCK_SIZE];
	bio_read(block_num, block);
	memcpy(inode, block+offset, sizeof(inode_t));

	return 0;
}

/* 
 * Writes to disk from *inode. Assuming ino is valid inode number.
 */
int writei(uint16_t ino, inode_t *inode) {
	int block_num = superblock.i_start_blk + ino / (BLOCK_SIZE/sizeof(inode_t));
	size_t offset = sizeof(inode_t) * (ino % (BLOCK_SIZE/sizeof(inode_t)));

	char block[BLOCK_SIZE];
	bio_read(block_num, block);
	memcpy(block+offset, inode, sizeof(inode_t));
	bio_write(block_num, block);

	return 0;
}


/************** Directory Operations **************/

/* 
 * Stores default values for dirent at *dirent. Note this function does not 
 * write to disk.
 */
void dirent_init(dirent_t *dirent, uint16_t ino, const char *name, size_t name_len) {
	dirent->valid = 1;
	dirent->ino = ino;
	strncpy(dirent->name, name, name_len);
	dirent->name[name_len] = 0;
	dirent->name_len = name_len;
}

/* 
 * Load inode from disk and searches through directory entries to find one 
 * that matches fname. On successful hit, copy dirent to *dirent_p and return
 * 0. If cannot find, return -1. Assume that ino is valid and is dir.
 */
int dir_find(uint16_t ino, const char *fname, size_t name_len, dirent_t *dirent_p) {
	inode_t inode;
	readi(ino, &inode);

	char block[BLOCK_SIZE];
	for (int i=0; i < 16; i++) {
		if (inode.direct_ptr[i] == -1)
			continue;
		
		bio_read(inode.direct_ptr[i], block);
		for (int j=0; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			dirent_t *dirent = (dirent_t*)block+j;  // pointer to dir entry in block
			if (dirent->valid == 1) {
				if (dirent->name_len == name_len && strncmp(dirent->name, fname, name_len) == 0) {
					memcpy(dirent_p, dirent, sizeof(dirent_t));
					return 0;
				}
			}
		}
	}
	return -1;
}

/* 
 * Attempts to add entry with ino f_ino and name fname into dir_inode. Checks
 * if an entry already exists with the same name and then checks if there's enough
 * space to add another entry. Return 0 on successful add and error code if 
 * there is an error. Assume that dir_inode points to valid inode.
 */
int dir_add(inode_t *dir_inode, uint16_t f_ino, const char *fname, size_t name_len) {
	/* Make sure dirent doesnt exist in directory */
	dirent_t dirent;
	if (dir_find(dir_inode->ino, fname, name_len, &dirent) == 0)
		return -EEXIST;
	
	char block[BLOCK_SIZE];
	for (int i=0; i < 16; i++) {
		/* If dirent block not initialized, allocate new one */
		if (dir_inode->direct_ptr[i] == -1) { 
			int blkno = get_avail_blkno();
			if (blkno == -1)
				return -ENOSPC;
			dir_inode->direct_ptr[i] = blkno;
			memset(block, 0, BLOCK_SIZE);
		}
		/* Otherwise read dirent block from disk */
		else {
			bio_read(dir_inode->direct_ptr[i], block);
		}

		/* Loop through dirent in block */
		for (int j=0; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			dirent_t *dirent = (dirent_t*)block+j;
			/* dirent is not valid, free to use */
			if (dirent->valid == 0) {
				dirent_init(dirent, f_ino, fname, name_len);
				bio_write(dir_inode->direct_ptr[i], block);  // persist changes to block
				return 0;
			}
		}
	}
	return -ENOSPC;
}

/* 
 * Attempts to remove entry with name fname from dir_inode. Returns 0 if entry is 
 * found and removed and -1 otherwise.
 */
int dir_remove(inode_t *dir_inode, const char *fname, size_t name_len) {
	char block[BLOCK_SIZE];
	for (int i=0; i < 16; i++) {
		if (dir_inode->direct_ptr[i] == -1)
			continue;

		bio_read(dir_inode->direct_ptr[i], block);
		for (int j=0; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			dirent_t *dirent = (dirent_t*)block+j;
			if (dirent->valid == 1) {
				if (dirent->name_len == name_len && strncmp(dirent->name, fname, name_len) == 0) {
					dirent->valid = 0;
					bio_write(dir_inode->direct_ptr[i], block);
					return 0;
				}
			}
		}
	}
	return -1;
}

/* 
 * Recursively search for inode at given path. Inital calls to this function
 * should use ino=ROOT_INO if path is given in terms of the root dir.
 * 
 * Sample Trace:
 * path = "/dir/subdir/file" : find "dir" under "/"
 * path = "/subdir/file" : find "subdir" under "dir"
 * path = "/file" : find "file" under "subdir"
 * Return inode corresponding to "file"
 */
int get_node_by_path(const char *path, uint16_t ino, inode_t *inode) {
	/* Check if path is just root dir "/" */
	if (strcmp(path, "/") == 0) {
		readi(ROOT_INO, inode);
		return 0;
	}

	/* Ignore first char if it is slash */
	if (*path == '/')
		path++;
	char *ptr = strchr(path, '/');  // everything before ptr is highest level name
	int len = (ptr == NULL) ? strlen(path) : ptr-path;  // length of highest level name

	dirent_t dirent;
	if (dir_find(ino, path, len, &dirent) == 0) {
		/* if end of path, read into inode and return */
		if (ptr == NULL) {
			readi(dirent.ino, inode);
			return 0;
		}
		else {
			return get_node_by_path(ptr, dirent.ino, inode);
		}
	}
	return -1;  // dir/file not found
}


/************** TFS Fuse Operations **************/

/* 
 * Initialize DISKFILE at diskfile_path, setup superblock structure and
 * info, setup bitmaps, and initialize root directory inode "/".
 */
int tfs_mkfs() {

	/* simple spin lock for thread safety */
	while (__sync_lock_test_and_set(&flag, 1) == 1) {
    }

	/* Initialize DISKFILE */
	dev_init(diskfile_path);

	/* Initialize superblock struct and info */
	superblock.magic_num = MAGIC_NUM;
	superblock.max_inum = MAX_INUM;
	superblock.max_dnum = MAX_DNUM;
	superblock.i_bitmap_blk = 1;
	superblock.d_bitmap_blk = 2;
	superblock.i_start_blk = superblock.d_bitmap_blk+1;
	int inode_per_blk = BLOCK_SIZE / sizeof(inode_t);
	superblock.d_start_blk = superblock.i_start_blk + (MAX_INUM+(inode_per_blk-1))/inode_per_blk;

	/* Write superblock to disk */
	char block[BLOCK_SIZE];
	memcpy(block, &superblock, sizeof(superblock_t));
	bio_write(0, block);

	/* Write bitmaps to disk */
	memset(block, 0, BLOCK_SIZE);
	bio_write(superblock.i_bitmap_blk, block);
	bio_write(superblock.d_bitmap_blk, block);

	/* Initialize '/' root inode and write to disk */
	inode_t inode;
	inode_init(&inode, get_avail_ino(), TYPE_DIR);
	writei(inode.ino, &inode);
	dir_add(&inode, inode.ino, ".", 1);
	writei(inode.ino, &inode);

	__sync_lock_test_and_set(&flag, 0);
	return 0;
}

static void *tfs_init(struct fuse_conn_info *conn) {
	if (access(diskfile_path, F_OK) == 0) {
		/* Load DISKFILE and read superblock */
		dev_open(diskfile_path);
		char block[BLOCK_SIZE];
		bio_read(0, block);
		memcpy(&superblock, block, sizeof(superblock_t));
	}
	else {
		/* Initialize DISKFILE, superblock will be initialized in tfs_mkfs() */
		tfs_mkfs();
	}
	return NULL;
}

static void tfs_destroy(void *userdata) {
	/* All in-memory structures are local vars, nothing to free() */
	dev_close();
}

static int tfs_getattr(const char *path, struct stat *stbuf) {
	/* Check dir/file at path exists */
	inode_t inode;
	if (get_node_by_path(path, ROOT_INO, &inode) == -1)
		return -ENOENT;
	
	/* Fill stbuf with inode info */
	memset(stbuf, 0, sizeof(*stbuf));
	stbuf->st_ino = inode.ino;  // not important
	stbuf->st_mode = (inode.type == TYPE_DIR ? S_IFDIR : S_IFREG) | 0755;
	stbuf->st_nlink = inode.link;
	stbuf->st_size = inode.size;
	stbuf->st_uid = getuid();
	stbuf->st_gid = getgid();
	time(&stbuf->st_mtime);

	return 0;
}

static int tfs_opendir(const char *path, struct fuse_file_info *fi) {
	inode_t inode;
	/* Check path exists and inode is DIR */
	if (get_node_by_path(path, ROOT_INO, &inode) == 0 && inode.type == TYPE_DIR) {
		return 0;
	}
	else {
		return -1;
	}
}

/* 
 * Finds inode at path and passes all valid dirents into function filler.
 */
static int tfs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
	/* Path doesnt exist or inode is type FILE */
	inode_t inode;
	if (get_node_by_path(path, ROOT_INO, &inode) == -1 || inode.type == TYPE_FILE)
		return -1;

	/* Loop through dirent blocks */
	char block[BLOCK_SIZE];
	for (int i=0; i < 16; i++) {
		if (inode.direct_ptr[i] == -1)
			continue;
	
		bio_read(inode.direct_ptr[i], block);
		for (int j=0; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			dirent_t *dirent = (dirent_t*)block+j;
			if (dirent->valid == 1) {  // found valid dir entry
				filler(buffer, dirent->name, NULL, 0);
			}
		}
	}
	return 0;
}

/* 
 * Helper function for tfs calls. Splits path into parent and target components,
 * where target is the name of the lowest file/dir and parent is the path of the
 * target's parent. Assume parent and target
 * 
 * "/dir/subdir/subsub/file" --> parent = "/dir/subdir/subsub", target = "file"
 */
void parse_name(const char *path, char *parent, char *target) {
	strncpy(parent, path, 4095);
	char *p = strrchr(path, '/');  // find last instance of '/'

	/* if last '/' is first char, null terminate after it. otherwise replace it with '\0' */
	parent[(p == path) ? 1 : p-path] = 0;
	strncpy(target, p+1, 207);
}

/* 
 * Tries to create directory at path. If fails for any reason will return 
 * corresponding error code. Otherwise return 0.
 */
static int tfs_mkdir(const char *path, mode_t mode) {
	/* split path into parent and target */
	char parent[4096], target[208];
	parse_name(path, parent, target);

	/* error checking */
	inode_t p_inode, t_inode;
	if (get_node_by_path(parent, ROOT_INO, &p_inode) == -1) {
		return -ENOENT;  /* parent doesnt exist */
	}
	if (p_inode.type != TYPE_DIR) { 
		return -ENOTDIR;  /* parent exists but isnt dir*/
	}

	int ino, retstat;
	if ((ino = get_avail_ino()) == -1) { 
		return -ENOSPC;  /* no space for inode */
	}
	if ((retstat = dir_add(&p_inode, ino, target, strlen(target))) < 0) {
		clear_bmap_ino(ino);
		return retstat;  /* dir_add() failed, probably no space for dirent */
	}

	while (__sync_lock_test_and_set(&flag, 1) == 1) {
    }
	/* write changes to parent */
	p_inode.link++;
	writei(p_inode.ino, &p_inode);

	/* initialize and write new inode */
	inode_init(&t_inode, ino, TYPE_DIR);
	writei(t_inode.ino, &t_inode);  // THIS LINE IS IMPORTANT, must clear out inode

	/* setup "." and ".." dirents */
	dir_add(&t_inode, t_inode.ino, ".", 1);
	dir_add(&t_inode, p_inode.ino, "..", 2);
	writei(t_inode.ino, &t_inode);

	__sync_lock_test_and_set(&flag, 0);
	return 0;
}

/* 
 * Tries to remove directory at path. If fails for any reason will return 
 * corresponding error code. Otherwise return 0.
 */
static int tfs_rmdir(const char *path) {
	char parent[4096], target[208];
	parse_name(path, parent, target);

	inode_t p_inode, t_inode;
	if (get_node_by_path(path, ROOT_INO, &t_inode) == -1)  // if target exists, parent must exist
		return -ENOENT;
	get_node_by_path(parent, ROOT_INO, &p_inode);
	
	while (__sync_lock_test_and_set(&flag, 1) == 1) {
    }
	/* clear entries in bitmap */
	for (int i=0; i < 16; i++) {
		if (t_inode.direct_ptr[i] != -1) {
			printf("%d\n", t_inode.direct_ptr[i]);
			char block[BLOCK_SIZE];
			memset(block, 0, BLOCK_SIZE);
			bio_write(t_inode.direct_ptr[i], block);
			clear_bmap_blkno(t_inode.direct_ptr[i]);
		}
	}
	clear_bmap_ino(t_inode.ino);

	/* invalidate inode and remove dirent from parent */
	t_inode.valid = 0;
	writei(t_inode.ino, &t_inode);
	dir_remove(&p_inode, target, strlen(target));

	__sync_lock_test_and_set(&flag, 0);
	return 0;
}

static int tfs_releasedir(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int tfs_open(const char *path, struct fuse_file_info *fi) {
	inode_t inode;
	/* Check path exists and inode is FILE */
	if (get_node_by_path(path, ROOT_INO, &inode) == 0 && inode.type == TYPE_FILE) {
		return 0;
	}
	else {
		return -1;
	}
}

/* 
 * Tries to create file at path. Very similar to tfs_mkdir. If fails 
 * for any reason will return corresponding error code. Otherwise return 0.
 */
static int tfs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
	char parent[4096], target[208];
	parse_name(path, parent, target);

	inode_t p_inode, t_inode;
	if (get_node_by_path(parent, ROOT_INO, &p_inode) == -1) {
		return -ENOENT;
	}
	if (p_inode.type != TYPE_DIR) { 
		return -ENOTDIR;
	}

	int ino, retstat;
	if ((ino = get_avail_ino()) == -1) { 
		return -ENOSPC;
	}
	if ((retstat = dir_add(&p_inode, ino, target, strlen(target))) < 0) {
		clear_bmap_ino(ino);
		return retstat;
	}

	while (__sync_lock_test_and_set(&flag, 1) == 1) {
    }
	writei(p_inode.ino, &p_inode);
	inode_init(&t_inode, ino, TYPE_FILE);
	writei(t_inode.ino, &t_inode);

	__sync_lock_test_and_set(&flag, 0);
	return 0;
}

/* 
 * Tries to remove file at path. Identical in function to tfs_rmdir. If fails 
 * for any reason will return corresponding error code. Otherwise return 0.
 */
static int tfs_unlink(const char *path) {
	char parent[4096], target[208];
	parse_name(path, parent, target);

	inode_t p_inode, t_inode;
	if (get_node_by_path(path, ROOT_INO, &t_inode) == -1)
		return -ENOENT;
	get_node_by_path(parent, ROOT_INO, &p_inode);
	
	while (__sync_lock_test_and_set(&flag, 1) == 1) {
    }
	for (int i=0; i < 16; i++) {
		if (t_inode.direct_ptr[i] != -1) {
			char block[BLOCK_SIZE];
			memset(block, 0, BLOCK_SIZE);
			bio_write(t_inode.direct_ptr[i], block);
			clear_bmap_blkno(t_inode.direct_ptr[i]);
		}
	}
	clear_bmap_ino(t_inode.ino);

	t_inode.valid = 0;
	writei(t_inode.ino, &t_inode);
	dir_remove(&p_inode, target, strlen(target));

	__sync_lock_test_and_set(&flag, 0);
	return 0;
}

/* 
 * Reads data from diskfile at path into buffer, given size and offsets. To
 * minimize disk I/O calls, we try to memcpy() block by block whenever possible,
 * and copy portions if the copied sections only cover part of blocks.
*/
static int tfs_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {
	inode_t inode;
	if (get_node_by_path(path, ROOT_INO, &inode) == -1)
		return -ENOENT;  /* path doesnt exist */
	if (inode.type != TYPE_FILE)
		return -EISDIR;  /* path points to dir not file */
	if (size+offset > BLOCK_SIZE * 16)
		return -EFBIG;  /* read too big */


	while (__sync_lock_test_and_set(&flag, 1) == 1) {
    }
	int start_block = offset / BLOCK_SIZE;
	int start_byte = offset % BLOCK_SIZE;
	int end_block = (offset + size) / BLOCK_SIZE;
	int end_byte = (offset + size) % BLOCK_SIZE;	

	char block[BLOCK_SIZE];
	bio_read(inode.direct_ptr[start_block], block);

	/* read first block */
	if (size <= BLOCK_SIZE - start_byte) {
		memcpy(buffer, block + start_byte, size);
		__sync_lock_test_and_set(&flag, 0);
		return size;
	}
	memcpy(buffer, block + start_byte, BLOCK_SIZE - start_byte);
	buffer +=  BLOCK_SIZE - start_byte;  // move buffer pointer to next addr to be read

	/* read middle blocks */
	for (int i=start_block+1; i < end_block; i++) {
		bio_read(inode.direct_ptr[i], block);
		memcpy(buffer, block, BLOCK_SIZE);
		buffer += BLOCK_SIZE;
	}

	/* read last block if section hangs over */
	if (end_byte > 0 && end_block > start_block) {
		bio_read(inode.direct_ptr[end_block], block);
		memcpy(buffer, block, end_byte);
	}

	__sync_lock_test_and_set(&flag, 0);
	return size;
}

/* 
 * Helper function for tfs_write(), checks if inode block at index i is valid.
 * If not, try to allocate a new block and store it in inode. Returns 0 on
 * success and -1 on failture.
 */
int check_and_alloc(inode_t *inode, int i) {
	if (inode->direct_ptr[i] >= 0)
		return 0;
		
	// Allocate a new data block if it does not exist
	int d_blk_num = get_avail_blkno();
	if (d_blk_num == -1) 
		return -1;

	char block[BLOCK_SIZE];
	memset(block, 0, BLOCK_SIZE);
	bio_write(d_blk_num, block);

	inode->direct_ptr[i] = d_blk_num;
	inode->size += BLOCK_SIZE;  // increment file size
	writei(inode->ino, inode);

	return 0;
}

/* 
 * Functionally very similar to tfs_read() but in reverse, moving data from
 * buffer to disk. Difference is we need to allocate data blocks for the file.
 * Will overwrite data that was previously on disk.
 */
static int tfs_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {
	inode_t inode;
	if (get_node_by_path(path, ROOT_INO, &inode) == -1)
		return -ENOENT;
	if (inode.type != TYPE_FILE)
		return -EISDIR;
	if (size+offset > BLOCK_SIZE * 16)
		return -EFBIG;
	

	while (__sync_lock_test_and_set(&flag, 1) == 1) {
    }
	int start_block = offset / BLOCK_SIZE;
	int start_byte = offset % BLOCK_SIZE;
	int end_block = (offset + size) / BLOCK_SIZE;
	int end_byte = (offset + size) % BLOCK_SIZE;

	if (check_and_alloc(&inode, start_block) == -1)
		return -ENOSPC;
	char block[BLOCK_SIZE];
	bio_read(inode.direct_ptr[start_block], block);

	/* first block */
	if (size <= BLOCK_SIZE - start_byte) {
		memcpy(block + start_byte, buffer, size);
		bio_write(inode.direct_ptr[start_block], block);
		__sync_lock_test_and_set(&flag, 0);
		return size;
	}
	memcpy(block + start_byte, buffer, BLOCK_SIZE - start_byte);
	bio_write(inode.direct_ptr[start_block], block);
	buffer +=  BLOCK_SIZE - start_byte;

	/* middle blocks */
	for (int i=start_block+1; i < end_block; i++) {
		if (check_and_alloc(&inode, i) == -1) {
			__sync_lock_test_and_set(&flag, 0);
			return -ENOSPC;
		}
		memcpy(block, buffer, BLOCK_SIZE);
		bio_write(inode.direct_ptr[i], buffer);
		buffer += BLOCK_SIZE;
	}

	/* last block */
	if (end_byte > 0 && end_block > start_block) {
		if (check_and_alloc(&inode, end_block) == -1) {
			__sync_lock_test_and_set(&flag, 0);
			return -ENOSPC;
		}
		bio_read(inode.direct_ptr[end_block], block);
		memcpy(block, buffer, end_byte);
		bio_write(inode.direct_ptr[end_block], block);
	}

	__sync_lock_test_and_set(&flag, 0);
	return size;
}

static int tfs_truncate(const char *path, off_t size) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int tfs_release(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
	return 0;
}

static int tfs_flush(const char * path, struct fuse_file_info * fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int tfs_utimens(const char *path, const struct timespec tv[2]) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}


static struct fuse_operations tfs_ope = {
	.init		= tfs_init,
	.destroy	= tfs_destroy,

	.getattr	= tfs_getattr,
	.readdir	= tfs_readdir,
	.opendir	= tfs_opendir,
	.releasedir	= tfs_releasedir,
	.mkdir		= tfs_mkdir,
	.rmdir		= tfs_rmdir,

	.create		= tfs_create,
	.open		= tfs_open,
	.read 		= tfs_read,
	.write		= tfs_write,
	.unlink		= tfs_unlink,

	.truncate   = tfs_truncate,
	.flush      = tfs_flush,
	.utimens    = tfs_utimens,
	.release	= tfs_release
};


int main(int argc, char **argv) {
	int fuse_stat;
	getcwd(diskfile_path, PATH_MAX);
	strcat(diskfile_path, "/DISKFILE");
	fuse_stat = fuse_main(argc, argv, &tfs_ope, NULL);
	return fuse_stat;
}

