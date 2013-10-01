/*
 *	file.c
 *
 *	file system operations
 *
 *	Copyright (c) 2010-2013, PostgreSQL Global Development Group
 *	contrib/pg_upgrade/file.c
 */

#include "postgres_fe.h"
#include "pg_config.h"

#include "pg_upgrade.h"

#include <fcntl.h>

#ifdef HAVE_LINUX_BTRFS_H
# include <sys/ioctl.h>
# include <linux/btrfs.h>
#endif


#ifndef WIN32
static int	copy_file(const char *fromfile, const char *tofile, bool force);
#else
static int	win32_pghardlink(const char *src, const char *dst);
#endif


/*
 * upgradeFile()
 *
 * Transfer a relation file from src to dst using one of the supported
 * methods.  If the on-disk format of the new cluster is bit-for-bit
 * compatible with the on-disk format of the old cluster we can simply link
 * each relation to perform a true in-place upgrade.  Otherwise we must copy
 * (either block-by-block or using a copy-on-write clone) the data from old
 * cluster to new cluster and then perform the conversion.
 */
const char *
upgradeFile(transferMode transfer_mode, const char *src,
		const char *dst, pageCnvCtx *pageConverter)
{
	if (pageConverter == NULL)
	{
		int rc = -1;

		switch (transfer_mode)
		{
			case TRANSFER_MODE_COPY:
				rc = pg_copy_file(src, dst, true);
				break;
			case TRANSFER_MODE_CLONE:
				rc = upg_clone_file(src, dst);
				break;
			case TRANSFER_MODE_LINK:
				rc = pg_link_file(src, dst);
				break;
		}

		return (rc < 0) ? getErrorText(errno) : NULL;
	}
	else if (transfer_mode != TRANSFER_MODE_COPY)
	{
		return "Cannot in-place update this cluster, "
			"page-by-page (copy-mode) conversion is required";
	}
	else
	{
		/*
		 * We have a pageConverter object - that implies that the
		 * PageLayoutVersion differs between the two clusters so we have to
		 * perform a page-by-page conversion.
		 *
		 * If the pageConverter can convert the entire file at once, invoke
		 * that plugin function, otherwise, read each page in the relation
		 * file and call the convertPage plugin function.
		 */

#ifdef PAGE_CONVERSION
		if (pageConverter->convertFile)
			return pageConverter->convertFile(pageConverter->pluginData,
											  dst, src);
		else
#endif
		{
			int			src_fd;
			int			dstfd;
			char		buf[BLCKSZ];
			ssize_t		bytesRead;
			const char *msg = NULL;

			if ((src_fd = open(src, O_RDONLY, 0)) < 0)
				return "could not open source file";

			if ((dstfd = open(dst, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR)) < 0)
			{
				close(src_fd);
				return "could not create destination file";
			}

			while ((bytesRead = read(src_fd, buf, BLCKSZ)) == BLCKSZ)
			{
#ifdef PAGE_CONVERSION
				if ((msg = pageConverter->convertPage(pageConverter->pluginData, buf, buf)) != NULL)
					break;
#endif
				if (write(dstfd, buf, BLCKSZ) != BLCKSZ)
				{
					msg = "could not write new page to destination";
					break;
				}
			}

			close(src_fd);
			close(dstfd);

			if (msg)
				return msg;
			else if (bytesRead != 0)
				return "found partial page in source file";
			else
				return NULL;
		}
	}
}


#ifndef WIN32
static int
copy_file(const char *srcfile, const char *dstfile, bool force)
{
#define COPY_BUF_SIZE (50 * BLCKSZ)

	int			src_fd;
	int			dest_fd;
	char	   *buffer;
	int			ret = 0;
	int			save_errno = 0;

	if ((srcfile == NULL) || (dstfile == NULL))
		return -1;

	if ((src_fd = open(srcfile, O_RDONLY, 0)) < 0)
		return -1;

	if ((dest_fd = open(dstfile, O_RDWR | O_CREAT | (force ? 0 : O_EXCL), S_IRUSR | S_IWUSR)) < 0)
	{
		if (src_fd != 0)
			close(src_fd);

		return -1;
	}

	buffer = (char *) pg_malloc(COPY_BUF_SIZE);

	/* perform data copying i.e read src source, write to destination */
	while (true)
	{
		ssize_t		nbytes = read(src_fd, buffer, COPY_BUF_SIZE);

		if (nbytes < 0)
		{
			save_errno = errno;
			ret = -1;
			break;
		}

		if (nbytes == 0)
			break;

		errno = 0;

		if (write(dest_fd, buffer, nbytes) != nbytes)
		{
			save_errno = errno;
			ret = -1;
			break;
		}
	}

	pg_free(buffer);

	if (src_fd != 0)
		close(src_fd);

	if (dest_fd != 0)
		close(dest_fd);

	if (save_errno != 0)
		errno = save_errno;

	return ret;
}
#endif


void
check_hard_link(void)
{
	char		existing_file[MAXPGPATH];
	char		new_link_file[MAXPGPATH];

	snprintf(existing_file, sizeof(existing_file), "%s/PG_VERSION", old_cluster.pgdata);
	snprintf(new_link_file, sizeof(new_link_file), "%s/PG_VERSION.linktest", new_cluster.pgdata);
	unlink(new_link_file);		/* might fail */

	if (pg_link_file(existing_file, new_link_file) == -1)
	{
		pg_log(PG_FATAL,
			   "Could not create hard link between old and new data directories: %s\n"
			   "In link mode the old and new data directories must be on the same file system volume.\n",
			   getErrorText(errno));
	}
	unlink(new_link_file);
}

#ifdef WIN32
static int
win32_pghardlink(const char *src, const char *dst)
{
	/*
	 * CreateHardLinkA returns zero for failure
	 * http://msdn.microsoft.com/en-us/library/aa363860(VS.85).aspx
	 */
	if (CreateHardLinkA(dst, src, NULL) == 0)
		return -1;
	else
		return 0;
}
#endif


int
upg_clone_file(const char *existing_file, const char *new_file)
{
#ifdef BTRFS_IOC_CLONE
	int rc, res_errno = 0, src_fd = -1, dest_fd = -1;

	src_fd = open(existing_file, O_RDONLY);
	if (src_fd < 0)
		return -1;

	dest_fd = open(new_file, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
	if (dest_fd < 0)
	{
		close(src_fd);
		return -1;
	}

	rc = ioctl(dest_fd, BTRFS_IOC_CLONE, src_fd);
	if (rc < 0)
	{
		pg_log(PG_REPORT, "btrfs clone: %s\n", strerror(errno));
		res_errno = errno;  /* save errno for caller */
		unlink(new_file);
	}

	close(dest_fd);
	close(src_fd);

	errno = res_errno;  /* restore errno after close() calls */
	return rc;
#else
	/* TODO: add support for zfs clones */
	pg_log(PG_REPORT, "system does not support file cloning\n");
	errno = ENOSYS;
	return -1;
#endif
}

void
check_clone_file(void)
{
	char		existing_file[MAXPGPATH];
	char		cloned_file[MAXPGPATH];

	snprintf(existing_file, sizeof(existing_file), "%s/PG_VERSION", old_cluster.pgdata);
	snprintf(cloned_file, sizeof(cloned_file), "%s/PG_VERSION.linktest", new_cluster.pgdata);
	unlink(cloned_file);		/* might fail */

	if (upg_clone_file(existing_file, cloned_file) == -1)
	{
		pg_log(PG_FATAL,
			   "Could not clone a file between old and new data directories: %s\n"
			   "File cloning is currently only supported on btrfs.\n",
			   getErrorText(errno));
	}
	unlink(cloned_file);
}

/* fopen() file with no group/other permissions */
FILE *
fopen_priv(const char *path, const char *mode)
{
	mode_t		old_umask = umask(S_IRWXG | S_IRWXO);
	FILE	   *fp;

	fp = fopen(path, mode);
	umask(old_umask);

	return fp;
}
