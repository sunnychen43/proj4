/*
 *  Copyright (C) 2021 CS416 Rutgers CS
 *	Tiny File System
 *	File:	tfs.c
 *
 */

#define FUSE_USE_VERSION 26

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
				return index;
			}
		}
	}

	return 0;
}

/* 
 * inode operations
 */
int readi(uint16_t ino, inode_t *inode) {

	int block_num = ino / (BLOCK_SIZE/sizeof(inode_t));
	size_t offset = sizeof(inode_t) * (ino % (BLOCK_SIZE/sizeof(inode_t)));

	char block[BLOCK_SIZE];
	bio_read(block_num, block);
	memcpy(inode, block+offset, sizeof(inode_t));

	return 0;
}

int writei(uint16_t ino, inode_t *inode) {

	int block_num = ino / (BLOCK_SIZE/sizeof(inode_t));
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
int dir_find(uint16_t ino, const char *fname, size_t name_len, struct dirent *dirent) {

	inode_t inode;
	readi(ino, &inode);

	for (int i=0; i < 16; i++) {
		if (inode.direct_ptr[i] == 0) {
			break;
		}

		char block[BLOCK_SIZE];
		bio_read(inode.direct_ptr[i], block);
		dirent_t *dirent_blk = block;
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

int dir_add(inode_t dir_inode, uint16_t f_ino, const char *fname, size_t name_len) {
	char block[BLOCK_SIZE];

	for (int i=0; i < 16; i++) {
		// if no data block allocate one
		if (dir_inode.direct_ptr[i] == 0) {
			memset(block, 0, BLOCK_SIZE);

			dirent_t *dirent = block;
			dirent->ino = f_ino;
			dirent->valid = 1;
			memcpy(dirent->name, fname, name_len);
			dirent->name_len = name_len;

			int block_num = get_avail_blkno();
			dir_inode.direct_ptr[i] = block_num;
			bio_write(block_num, block);
			return 0;
		}

		bio_read(inode.direct_ptr[i], block);
		dirent_t *dirent_blk = block;
		for (int j=0; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			if (dirent_blk[j].valid == 0) {
				dirent_t *dirent = dirent_blk+j;
				dirent->ino = f_ino;
				dirent->valid = 1;
				memcpy(dirent->name, fname, name_len);
				dirent->name_len = name_len;

				bio_write(inode.direct_ptr[i], block);
				return 0;
			}
			else if (dirent_blk[j].name_len == name_len && strncmp(dirent_blk[j].name, fname, name_len) == 0) {
				return 1;
			}
		}
	}

	return 1;
}

int dir_remove(inode_t dir_inode, const char *fname, size_t name_len) {

	char block[BLOCK_SIZE];

	for (int i=0; i < 16; i++) {
		// if no data block allocate one
		if (dir_inode.direct_ptr[i] == 0) {
			break;
		}

		bio_read(inode.direct_ptr[i], block);
		dirent_t *dirent_blk = block;
		for (int j=0; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			if (dirent_blk[j].valid == 1) {
				if (dirent_blk[j].name_len == name_len && strncmp(dirent_blk[j].name, fname, name_len) == 0) {
					dirent_blk[j].valid = 0;
					bio_write(dir_inode.direct_ptr[i], block);
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

/* 
 * Make file system
 */
int tfs_mkfs() {

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
// static void *tfs_init(struct fuse_conn_info *conn) {

// 	// Step 1a: If disk file is not found, call mkfs

//   // Step 1b: If disk file is found, just initialize in-memory data structures
//   // and read superblock from disk

// 	return NULL;
// }

// static void tfs_destroy(void *userdata) {

// 	// Step 1: De-allocate in-memory data structures

// 	// Step 2: Close diskfile

// }

// static int tfs_getattr(const char *path, struct stat *stbuf) {

// 	// Step 1: call get_node_by_path() to get inode from path

// 	// Step 2: fill attribute of file into stbuf from inode

// 		stbuf->st_mode   = S_IFDIR | 0755;
// 		stbuf->st_nlink  = 2;
// 		time(&stbuf->st_mtime);

// 	return 0;
// }

// static int tfs_opendir(const char *path, struct fuse_file_info *fi) {

// 	// Step 1: Call get_node_by_path() to get inode from path

// 	// Step 2: If not find, return -1

//     return 0;
// }

// static int tfs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {

// 	// Step 1: Call get_node_by_path() to get inode from path

// 	// Step 2: Read directory entries from its data blocks, and copy them to filler

// 	return 0;
// }


// static int tfs_mkdir(const char *path, mode_t mode) {

// 	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name

// 	// Step 2: Call get_node_by_path() to get inode of parent directory

// 	// Step 3: Call get_avail_ino() to get an available inode number

// 	// Step 4: Call dir_add() to add directory entry of target directory to parent directory

// 	// Step 5: Update inode for target directory

// 	// Step 6: Call writei() to write inode to disk
	

// 	return 0;
// }

// static int tfs_rmdir(const char *path) {

// 	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name

// 	// Step 2: Call get_node_by_path() to get inode of target directory

// 	// Step 3: Clear data block bitmap of target directory

// 	// Step 4: Clear inode bitmap and its data block

// 	// Step 5: Call get_node_by_path() to get inode of parent directory

// 	// Step 6: Call dir_remove() to remove directory entry of target directory in its parent directory

// 	return 0;
// }

// static int tfs_releasedir(const char *path, struct fuse_file_info *fi) {
// 	// For this project, you don't need to fill this function
// 	// But DO NOT DELETE IT!
//     return 0;
// }

// static int tfs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {

// 	// Step 1: Use dirname() and basename() to separate parent directory path and target file name

// 	// Step 2: Call get_node_by_path() to get inode of parent directory

// 	// Step 3: Call get_avail_ino() to get an available inode number

// 	// Step 4: Call dir_add() to add directory entry of target file to parent directory

// 	// Step 5: Update inode for target file

// 	// Step 6: Call writei() to write inode to disk

// 	return 0;
// }

// static int tfs_open(const char *path, struct fuse_file_info *fi) {

// 	// Step 1: Call get_node_by_path() to get inode from path

// 	// Step 2: If not find, return -1

// 	return 0;
// }

// static int tfs_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {

// 	// Step 1: You could call get_node_by_path() to get inode from path

// 	// Step 2: Based on size and offset, read its data blocks from disk

// 	// Step 3: copy the correct amount of data from offset to buffer

// 	// Note: this function should return the amount of bytes you copied to buffer
// 	return 0;
// }

// static int tfs_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {
// 	// Step 1: You could call get_node_by_path() to get inode from path

// 	// Step 2: Based on size and offset, read its data blocks from disk

// 	// Step 3: Write the correct amount of data from offset to disk

// 	// Step 4: Update the inode info and write it to disk

// 	// Note: this function should return the amount of bytes you write to disk
// 	return size;
// }

// static int tfs_unlink(const char *path) {

// 	// Step 1: Use dirname() and basename() to separate parent directory path and target file name

// 	// Step 2: Call get_node_by_path() to get inode of target file

// 	// Step 3: Clear data block bitmap of target file

// 	// Step 4: Clear inode bitmap and its data block

// 	// Step 5: Call get_node_by_path() to get inode of parent directory

// 	// Step 6: Call dir_remove() to remove directory entry of target file in its parent directory

// 	return 0;
// }

// static int tfs_truncate(const char *path, off_t size) {
// 	// For this project, you don't need to fill this function
// 	// But DO NOT DELETE IT!
//     return 0;
// }

// static int tfs_release(const char *path, struct fuse_file_info *fi) {
// 	// For this project, you don't need to fill this function
// 	// But DO NOT DELETE IT!
// 	return 0;
// }

// static int tfs_flush(const char * path, struct fuse_file_info * fi) {
// 	// For this project, you don't need to fill this function
// 	// But DO NOT DELETE IT!
//     return 0;
// }

// static int tfs_utimens(const char *path, const struct timespec tv[2]) {
// 	// For this project, you don't need to fill this function
// 	// But DO NOT DELETE IT!
//     return 0;
// }


// static struct fuse_operations tfs_ope = {
// 	.init		= tfs_init,
// 	.destroy	= tfs_destroy,

// 	.getattr	= tfs_getattr,
// 	.readdir	= tfs_readdir,
// 	.opendir	= tfs_opendir,
// 	.releasedir	= tfs_releasedir,
// 	.mkdir		= tfs_mkdir,
// 	.rmdir		= tfs_rmdir,

// 	.create		= tfs_create,
// 	.open		= tfs_open,
// 	.read 		= tfs_read,
// 	.write		= tfs_write,
// 	.unlink		= tfs_unlink,

// 	.truncate   = tfs_truncate,
// 	.flush      = tfs_flush,
// 	.utimens    = tfs_utimens,
// 	.release	= tfs_release
// };


int main(int argc, char **argv) {
	dev_init("./DISKFILE");
	char buf[4096];
	memset(buf, 0, 4096);
	superblock.i_bitmap_blk = 1;
	bio_write(1, buf);

	for (int i=0; i < 100; i++) {
		printf("%d\n", get_avail_ino());
	}

	return 0;

	// int fuse_stat;

	// getcwd(diskfile_path, PATH_MAX);
	// strcat(diskfile_path, "/DISKFILE");

	// fuse_stat = fuse_main(argc, argv, &tfs_ope, NULL);

	// return fuse_stat;
}

