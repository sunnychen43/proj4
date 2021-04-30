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

#include "block.h"
#include "tfs.h"

char diskfile_path[PATH_MAX];

// Declare your in-memory data structures here
static superblock_t superblock;
static inode_t inode;
static dirent_t dirent;

void clear_bmap_ino(int i) {
	char block[BLOCK_SIZE];
	bio_read(superblock.i_bitmap_blk, block);
	unset_bitmap(block, i);
	bio_write(superblock.i_bitmap_blk, block);
}

void clear_bmap_blkno(int i) {
	char block[BLOCK_SIZE];
	bio_read(superblock.d_bitmap_blk, block);
	unset_bitmap(block, i);
	bio_write(superblock.d_bitmap_blk, block);
}

/* 
 * Get available inode number from bitmap
 */
int get_avail_ino() {

	char block[BLOCK_SIZE];
	bio_read(superblock.i_bitmap_blk, block);

	for (int i=0; i < MAX_INUM/8; i++) {
		if (block[i] == ~0)
			continue;
		for (int j=0; j < 8; j++) {
			int index = i*8+j;
			if (get_bitmap(block, index) == 0) {
				set_bitmap(block, index);
				bio_write(superblock.i_bitmap_blk, block);
				return index;
			}
		}
	}

	return 0;
}

/* 
 * Get available data block number from bitmap
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

	return 0;
}

/* 
 * inode operations
 */
int readi(uint16_t ino, inode_t *inode) {

	int block_num = superblock.i_start_blk + ino / (BLOCK_SIZE/sizeof(inode_t));
	size_t offset = sizeof(inode_t) * (ino % (BLOCK_SIZE/sizeof(inode_t)));

	char block[BLOCK_SIZE];
	bio_read(block_num, block);
	memcpy(inode, block+offset, sizeof(inode_t));

	return 0;
}

int writei(uint16_t ino, inode_t *inode) {

	int block_num = superblock.i_start_blk + ino / (BLOCK_SIZE/sizeof(inode_t));
	size_t offset = sizeof(inode_t) * (ino % (BLOCK_SIZE/sizeof(inode_t)));

	char block[BLOCK_SIZE];
	bio_read(block_num, block);
	memcpy(block+offset, inode, sizeof(inode_t));
	bio_write(block_num, block);

	return 0;
}


/* 
 * directory operations
 */
int dir_find(uint16_t ino, const char *fname, size_t name_len, dirent_t *dirent) {

	inode_t inode;
	readi(ino, &inode);

	for (int i=0; i < 16; i++) {
		if (inode.direct_ptr[i] == 0) {
			continue;
		}

		char block[BLOCK_SIZE];
		bio_read(inode.direct_ptr[i], block);
		dirent_t *dirent_blk = (dirent_t*)block;
		for (int j=0; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			if (dirent_blk[j].valid == 1) {
				if (dirent_blk[j].name_len == name_len && strncmp(dirent_blk[j].name, fname, name_len) == 0) {
					memcpy(dirent, dirent_blk+j, sizeof(dirent_t));
					return 0;
				}
			}
		}
	}

	return 1;
}

int dir_add(inode_t *dir_inode, uint16_t f_ino, const char *fname, size_t name_len) {
	if (dir_find(dir_inode->ino, fname, name_len, &dirent) == 0)
		return 1;

	char block[BLOCK_SIZE];
	int i, j;
	for (i=0; i < 16; i++) {
		j = 0;
		if (dir_inode->direct_ptr[i] == 0) {  // if no data block allocate one
			dir_inode->direct_ptr[i] = get_avail_blkno();
			memset(block, 0, BLOCK_SIZE);
			bio_write(dir_inode->direct_ptr[i], block);
			break;
		}

		bio_read(inode.direct_ptr[i], block);
		for (; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			if (((dirent_t*)block)[j].valid == 0)
				break;
		}
	}

	if (i < 16) {
		dirent_t *dirent = (dirent_t*)block+j;
		dirent->ino = f_ino;
		dirent->valid = 1;
		memcpy(dirent->name, fname, name_len+1);
		dirent->name_len = name_len;
		bio_write(inode.direct_ptr[i], block);
		return 0;
	}
	else {  // no space in blocks
		return 1;
	}
}

int dir_remove(inode_t *dir_inode, const char *fname, size_t name_len) {

	char block[BLOCK_SIZE];

	for (int i=0; i < 16; i++) {
		// if no data block allocate one
		if (dir_inode->direct_ptr[i] == 0) {
			continue;
		}

		bio_read(inode.direct_ptr[i], block);
		dirent_t *dirent_blk = (dirent_t*)block;
		for (int j=0; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			if (dirent_blk[j].valid == 1) {
				if (dirent_blk[j].name_len == name_len && strncmp(dirent_blk[j].name, fname, name_len) == 0) {
					dirent_blk[j].valid = 0;
					bio_write(dir_inode->direct_ptr[i], block);
					return 0;
				}
			}
		}
	}

	return 1;
}

/* 
 * namei operation
 */
int get_node_by_path(const char *path, uint16_t ino, inode_t *inode) {
	if (strcmp(path, "/") == 0) {
		readi(superblock.i_start_blk, inode);
		return 0;
	}
	
	int len;
	char *ptr = strchr(path, '/');
	if (ptr == NULL) {
		len = strlen(ptr);
	}
	else {
		len = ptr-path;
	}

	dirent_t dirent;
	if (dir_find(ino, path, len, &dirent) == 0) {
		if (*(ptr+1) == 0) {  // end of path
			readi(dirent.ino, inode);
			return 0;
		}
		else {
			return get_node_by_path(ptr+1, dirent.ino, inode);
		}
	}

	return 1;  // not found
}

void inode_init(inode_t *inode, uint16_t ino, uint32_t type) {
	inode->ino = ino;
	inode->type = type;

	inode->valid = 1;
	inode->size = 0;
	inode->link = (type == 0) ? 2 : 1;
	for (int i=0; i < 16; i++) {
		inode->direct_ptr[i] = 0;
	}
}

/* 
 * Make file system
 */
int tfs_mkfs() {

	dev_init(diskfile_path);

	superblock.magic_num = MAGIC_NUM;
	superblock.max_inum = MAX_INUM;
	superblock.max_dnum = MAX_DNUM;
	superblock.i_bitmap_blk = 1;
	superblock.d_bitmap_blk = 2;
	superblock.i_start_blk = superblock.d_bitmap_blk+1;
	int inode_per_blk = BLOCK_SIZE / sizeof(inode_t);
	superblock.d_start_blk = superblock.i_start_blk + MAX_INUM/inode_per_blk;

	char block[BLOCK_SIZE];
	memcpy(block, &superblock, sizeof(superblock_t));
	bio_write(0, block);

	memset(block, 0, BLOCK_SIZE);
	set_bitmap(block, 0);  // reserve first bit
	bio_write(superblock.i_bitmap_blk, block);
	bio_write(superblock.i_bitmap_blk, block);

	inode_init(&inode, 1, 0);
	inode.link = 1;
	writei(inode.ino, &inode);

	// Call dev_init() to initialize (Create) Diskfile

	// write superblock information

	// initialize inode bitmap

	// initialize data block bitmap

	// update bitmap information for root directory

	// update inode for root directory

	return 0;
}


/* 
 * FUSE file operations
 */
static void *tfs_init(struct fuse_conn_info *conn) {

	if (access(diskfile_path, F_OK) == 0) {
		dev_open(diskfile_path);
		char block[BLOCK_SIZE];
		bio_read(0, block);
		memcpy(&superblock, block, sizeof(superblock_t));
	}
	else {
		tfs_mkfs();
	}

	return NULL;
}

static void tfs_destroy(void *userdata) {
	dev_close();
}

static int tfs_getattr(const char *path, struct stat *stbuf) {
	printf("%s\n", path);



	// Step 1: call get_node_by_path() to get inode from path

	// Step 2: fill attribute of file into stbuf from inode

		stbuf->st_mode   = S_IFDIR | 0755;
		stbuf->st_nlink  = 2;
		time(&stbuf->st_mtime);

	return 0;
}

static int tfs_opendir(const char *path, struct fuse_file_info *fi) {
    return get_node_by_path(path, 1, &inode) == 0 && inode.type == 0;
}

static int tfs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {

	get_node_by_path(path, 1, &inode);
	for (int i=0; i < 16; i++) {
		if (inode.direct_ptr[i] == 0)
			break;
		char block[BLOCK_SIZE];
		bio_read(inode.direct_ptr[i], block);
		dirent_t *dirent_blk = (dirent_t*)block;
		for (int j=0; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			if (dirent_blk[j].valid == 1) {
				filler(buffer, dirent_blk[j].name, NULL, 0);
			}
		}
	}

	return 0;
}


void parse_name(const char *path, char *parent, char *target) {
	char *p = strrchr(path, '/');
	strcpy(parent, path);
	parent[p-path] = 0;
	strcpy(target, p+1);
}

static int tfs_mkdir(const char *path, mode_t mode) {
	char parent[200], target[50];
	parse_name(path, parent, target);

	get_node_by_path(parent, 1, &inode);

	int ino = get_avail_ino();
	dir_add(&inode, ino, target, strlen(target));
	writei(inode.ino, &inode);

	inode_init(&inode, ino, 0);
	writei(ino, &inode);

	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name

	// Step 2: Call get_node_by_path() to get inode of parent directory

	// Step 3: Call get_avail_ino() to get an available inode number

	// Step 4: Call dir_add() to add directory entry of target directory to parent directory

	// Step 5: Update inode for target directory

	// Step 6: Call writei() to write inode to disk
	
	printf("%s\n", path);
	return 0;
}

static int tfs_rmdir(const char *path) {

	char parent[200], target[50];
	parse_name(path, parent, target);

	get_node_by_path(path, 1, &inode);
	for (int i=0; i < 16; i++) {
		if (inode.direct_ptr[i] != 0) {
			clear_bmap_blkno(inode.direct_ptr[i]);
		}
	}
	clear_bmap_ino(inode.ino);

	get_node_by_path(parent, 1, &inode);
	dir_remove(&inode, target, strlen(target));

	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name

	// Step 2: Call get_node_by_path() to get inode of target directory

	// Step 3: Clear data block bitmap of target directory

	// Step 4: Clear inode bitmap and its data block

	// Step 5: Call get_node_by_path() to get inode of parent directory

	// Step 6: Call dir_remove() to remove directory entry of target directory in its parent directory

	printf("%s\n", path);
	return 0;
}

static int tfs_releasedir(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
	printf("%s\n", path);
    return 0;
}

static int tfs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target file name

	// Step 2: Call get_node_by_path() to get inode of parent directory

	// Step 3: Call get_avail_ino() to get an available inode number

	// Step 4: Call dir_add() to add directory entry of target file to parent directory

	// Step 5: Update inode for target file

	// Step 6: Call writei() to write inode to disk
	printf("%s\n", path);
	return 0;
}

static int tfs_open(const char *path, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path

	// Step 2: If not find, return -1
	printf("%s\n", path);
	return 0;
}

static int tfs_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {

	// Step 1: You could call get_node_by_path() to get inode from path

	// Step 2: Based on size and offset, read its data blocks from disk

	// Step 3: copy the correct amount of data from offset to buffer

	// Note: this function should return the amount of bytes you copied to buffer
	printf("%s\n", path);
	return 0;
}

static int tfs_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {
	// Step 1: You could call get_node_by_path() to get inode from path

	// Step 2: Based on size and offset, read its data blocks from disk

	// Step 3: Write the correct amount of data from offset to disk

	// Step 4: Update the inode info and write it to disk

	// Note: this function should return the amount of bytes you write to disk
	printf("%s\n", path);
	return size;
}

static int tfs_unlink(const char *path) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target file name

	// Step 2: Call get_node_by_path() to get inode of target file

	// Step 3: Clear data block bitmap of target file

	// Step 4: Clear inode bitmap and its data block

	// Step 5: Call get_node_by_path() to get inode of parent directory

	// Step 6: Call dir_remove() to remove directory entry of target file in its parent directory
	printf("%s\n", path);
	return 0;
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
	// strcpy(diskfile_path, "./DISKFILE");
	// tfs_mkfs();
	// printf("%d\n", superblock.d_start_blk);
	// for (int i=0; i < 100; i++) {
	// 	printf("%d\n", get_avail_blkno());
	// }

	// printf("%d\n", tfs_opendir("/.", NULL));

	// return 0;

	// int fuse_stat;

	// getcwd(diskfile_path, PATH_MAX);
	// strcat(diskfile_path, "/DISKFILE");

	// fuse_stat = fuse_main(argc, argv, &tfs_ope, NULL);

	// return fuse_stat;

	char a[100], b[100];
	parse_name("/", a, b);
	printf("%s %s\n", a, b);
	return 0;
}

