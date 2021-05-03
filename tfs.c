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
#define ROOT_INO 0

char diskfile_path[PATH_MAX];

// Declare your in-memory data structures here
static superblock_t superblock;


int readi(uint16_t ino, inode_t *inode);
void print_dirent(uint16_t ino) {
	inode_t inode;
	readi(ino, &inode);
	char block[BLOCK_SIZE];
	for (int i=0; i < 16; i++) {
		// if no data block allocate one
		if (inode.direct_ptr[i] == -1)
			continue;
		bio_read(inode.direct_ptr[i], block);
		dirent_t *dirent_blk = (dirent_t*)block;
		for (int j=0; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			if (dirent_blk[j].valid == 1) {
				printf("%s\n", dirent_blk[j].name);
			}
		}
	}
}


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

	return -1;
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

	return -1;
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
void dirent_init(dirent_t *dirent, uint16_t ino, const char *name) {
	dirent->valid = 1;
	dirent->ino = ino;
	strcpy(dirent->name, name);
	dirent->name_len = strlen(name);
}

// NAME IS NOT NULL TERMINATED
int dir_find(uint16_t ino, const char *fname, size_t name_len, dirent_t *dirent) {
	inode_t inode;
	readi(ino, &inode);
	for (int i=0; i < 16; i++) {
		if (inode.direct_ptr[i] == -1)
			continue;
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

	return -1;
}

int dir_add(inode_t *dir_inode, uint16_t f_ino, const char *fname, size_t name_len) {
	dirent_t dirent;
	if (dir_find(dir_inode->ino, fname, name_len, &dirent) == 0)
		return -1;

	char block[BLOCK_SIZE];
	int i, j;
	bool run = true;
	for (i=0; i < 16; i++) {
		j = 0;
		if (dir_inode->direct_ptr[i] == -1) {  // if no data block allocate one
			dir_inode->direct_ptr[i] = get_avail_blkno();
			memset(block, 0, BLOCK_SIZE);
			bio_write(dir_inode->direct_ptr[i], block);
			break;
		}

		bio_read(dir_inode->direct_ptr[i], block);
		for (; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			if (((dirent_t*)block)[j].valid == 0) {
				run = false;
				break;
			}
		}
		if (run == false)
			break;
	}

	if (i < 16) {
		dirent_t *dirent = (dirent_t*)block+j;
		dirent_init(dirent, f_ino, fname);
		bio_write(dir_inode->direct_ptr[i], block);
		dir_inode->link++;
		return 0;
	}
	else {  // no space in blocks
		return -1;
	}
}

int dir_remove(inode_t *dir_inode, const char *fname, size_t name_len) {

	char block[BLOCK_SIZE];
	for (int i=0; i < 16; i++) {
		// if no data block allocate one
		if (dir_inode->direct_ptr[i] == -1)
			continue;
		bio_read(dir_inode->direct_ptr[i], block);
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

	return -1;
}

/* 
 * namei operation
 */
int get_node_by_path(const char *path, uint16_t ino, inode_t *inode) {
	if (strcmp(path, "/") == 0) {
		readi(ROOT_INO, inode);
		return 0;
	}
	
	if (*path == '/')
		path++;

	char *ptr = strchr(path, '/');
	int len = (ptr == NULL) ? strlen(path) : ptr-path;

	dirent_t dirent;
	if (dir_find(ino, path, len, &dirent) == 0) {
		if (ptr == NULL) {  // end of path
			readi(dirent.ino, inode);
			return 0;
		}
		else {
			return get_node_by_path(ptr, dirent.ino, inode);
		}
	}

	return -1;  // not found
}

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

	// clear bitmaps
	memset(block, 0, BLOCK_SIZE);
	bio_write(superblock.i_bitmap_blk, block);
	bio_write(superblock.d_bitmap_blk, block);

	// write root inode
	inode_t inode;
	int ino = get_avail_ino();
	inode_init(&inode, ino, 0);
	writei(ino, &inode);
	dir_add(&inode, ino, ".", 1);
	writei(ino, &inode);

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
	inode_t inode;
	if (get_node_by_path(path, ROOT_INO, &inode) == -1) {
		return -ENOENT;
	}

	memset(stbuf, 0, sizeof(*stbuf));

	stbuf->st_ino = inode.ino;
	stbuf->st_mode = (inode.type == 0 ? S_IFDIR : S_IFREG) | 0755;
	stbuf->st_nlink = inode.link;
	stbuf->st_size = inode.size;
	stbuf->st_uid = getuid();
	stbuf->st_gid = getgid();
	time(&stbuf->st_mtime);

	return 0;
}

static int tfs_opendir(const char *path, struct fuse_file_info *fi) {
	inode_t inode;
	if (get_node_by_path(path, ROOT_INO, &inode) == 0 && inode.type == 0) {
		return 0;
	}
	else {
		return -1;
	}
}

static int tfs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {

	inode_t inode;

	get_node_by_path(path, ROOT_INO, &inode);
	for (int i=0; i < 16; i++) {
		if (inode.direct_ptr[i] == -1)
			continue;
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
	strcpy(parent, path);

	char *p = strrchr(path, '/');
	parent[(p == path) ? 1 : p-path] = 0;
	strcpy(target, p+1);
}

static int tfs_mkdir(const char *path, mode_t mode) {

	char parent[200], target[50];
	parse_name(path, parent, target);

	inode_t p_inode, t_inode;
	get_node_by_path(parent, ROOT_INO, &p_inode);

	inode_init(&t_inode, get_avail_ino(), 0);
	dir_add(&t_inode, t_inode.ino, ".", 1);
	dir_add(&t_inode, p_inode.ino, "..", 2);
	writei(t_inode.ino, &t_inode);

	dir_add(&p_inode, t_inode.ino, target, strlen(target));
	writei(p_inode.ino, &p_inode);

	return 0;
}


static int tfs_rmdir(const char *path) {
	inode_t inode;

	char parent[200], target[50];
	parse_name(path, parent, target);

	if (get_node_by_path(path, ROOT_INO, &inode) == -1)
		return -1;
	
	for (int i=0; i < 16; i++) {
		clear_bmap_blkno(inode.direct_ptr[i]);
	}
	clear_bmap_ino(inode.ino);

	get_node_by_path(parent, ROOT_INO, &inode);
	dir_remove(&inode, target, strlen(target));

	printf("%s\n", path);
	return 0;
}

static int tfs_releasedir(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int tfs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
	inode_t inode;
	char parent[200], target[50];
	parse_name(path, parent, target);

	get_node_by_path(parent, ROOT_INO, &inode);

	int ino = get_avail_ino();
	dir_add(&inode, ino, target, strlen(target));
	writei(inode.ino, &inode);

	inode_init(&inode, ino, 1);
	writei(ino, &inode);
	
	return 0;
}

static int tfs_unlink(const char *path) {
	inode_t inode;
	char parent[200], target[50];
	parse_name(path, parent, target);

	if (get_node_by_path(path, ROOT_INO, &inode) == -1)
		return -1;
	
	for (int i=0; i < 16; i++) {
		clear_bmap_blkno(inode.direct_ptr[i]);
	}
	clear_bmap_ino(inode.ino);

	get_node_by_path(parent, ROOT_INO, &inode);
	dir_remove(&inode, target, strlen(target));

	return 0;
}

static int tfs_open(const char *path, struct fuse_file_info *fi) {
	inode_t inode;
	if (get_node_by_path(path, ROOT_INO, &inode) == 0 && inode.type == 1) {
		return 0;
	}
	else {
		return -1;
	}
}

static int tfs_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {

	inode_t inode;
	if (get_node_by_path(path, ROOT_INO, &inode) == -1)
		return -1;
	
	int start_block = offset / BLOCK_SIZE;
	int start_byte = offset % BLOCK_SIZE;
	int end_block = (offset + size) / BLOCK_SIZE;
	int end_byte = (offset + size) % BLOCK_SIZE;
	
	char block[BLOCK_SIZE];
	bio_read(inode.direct_ptr[start_block], block);
	//one block
	if (size <= BLOCK_SIZE - start_byte) {
		memcpy(buffer, block + start_byte, size);
		return size;
	}
	
	memcpy(buffer, block + start_byte, BLOCK_SIZE - start_byte);
	buffer +=  BLOCK_SIZE - start_byte;
	//middle blocks
	for (int i=start_block+1; i < end_block; i++) {
		bio_read(inode.direct_ptr[i], block);
		memcpy(buffer, block, BLOCK_SIZE);
		buffer += BLOCK_SIZE;
	}
	// last block
	if (end_byte > 0 && end_block > start_block) {
		bio_read(inode.direct_ptr[end_block], block);
		memcpy(buffer, block, end_byte);
	}
	return size;
}

int alloc_d_blk(inode_t *dir_inode, int i) {
	// Allocate a new data block for this directory since it does not exist
	int d_blk_num = get_avail_blkno();
	if (d_blk_num == -1) 
		return -1;

	char block[BLOCK_SIZE];
	memset(block, 0, BLOCK_SIZE);
	bio_write(d_blk_num, block);

	dir_inode->direct_ptr[i] = d_blk_num;
	dir_inode->size += BLOCK_SIZE;
	writei(dir_inode->ino, dir_inode);
	return 0;
}

static int tfs_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {
	
	inode_t inode;
	if (get_node_by_path(path, ROOT_INO, &inode) == -1) {
		printf("invalid file %s\n", path);
		return -1;
	}

	if (size > BLOCK_SIZE * 16) {
		printf("write too large\n");
		return -1;
	}
	
	int start_block = offset / BLOCK_SIZE;
	int start_byte = offset % BLOCK_SIZE;
	int end_block = (offset + size) / BLOCK_SIZE;
	int end_byte = (offset + size) % BLOCK_SIZE;


	if (inode.direct_ptr[start_block] == -1) {
		if (alloc_d_blk(&inode, start_block) == -1) {
			printf("start block allocation failed\n");
			return -1;
		}
	}
	
	char block[BLOCK_SIZE];
	bio_read(inode.direct_ptr[start_block], block);
	if (size <= BLOCK_SIZE - start_byte) {
		memcpy(block + start_byte, buffer, size);
		bio_write(inode.direct_ptr[start_block], block);
		return size;
	}
	memcpy(block + start_byte, buffer, BLOCK_SIZE - start_byte);
	buffer +=  BLOCK_SIZE - start_byte;
	//middle blocks
	for (int i=start_block+1; i < end_block; i++) {
		if (inode.direct_ptr[i] == -1) {
			if (alloc_d_blk(&inode, i) == -1) {
				printf("%d-th block allocation failed\n", i);
				return -1;
			}
		}
		memcpy(block, buffer, BLOCK_SIZE);
		bio_write(inode.direct_ptr[i], buffer);
		buffer += BLOCK_SIZE;
	}
	// last block
	if (end_byte > 0 && end_block > start_block) {
		if (inode.direct_ptr[end_block] == -1) {
			if (alloc_d_blk(&inode, end_block) == -1) {
				printf("end block allocation failed\n");
				return -1;
			}
		}
		bio_read(inode.direct_ptr[end_block], block);
		memcpy(block, buffer, end_byte);
		bio_write(inode.direct_ptr[end_block], block);
	}
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
	strcpy(diskfile_path, "./DISKFILE");
	tfs_mkfs();
	tfs_create("/a", 0755, NULL);

	char buf[100];
	for (int i=0; i < 100; i++) {
		buf[i] = i;
	}
	tfs_write("/a", buf, 100, 0, NULL);
	
	char buf2[100];
	tfs_read("/a", buf2, 100, 0, NULL);
	for (int i=0; i < 100; i++) {
		printf("%d\n", buf2[i]);
	}
	return 0;

	// int fuse_stat;
	// getcwd(diskfile_path, PATH_MAX);
	// strcat(diskfile_path, "/DISKFILE");
	// fuse_stat = fuse_main(argc, argv, &tfs_ope, NULL);
	// return fuse_stat;
}

