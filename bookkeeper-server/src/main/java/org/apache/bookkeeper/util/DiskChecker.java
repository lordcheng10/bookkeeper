/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.util;

import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 提供用于检查磁盘问题的实用功能的类。
 * Class that provides utility functions for checking disk problems.
 */
public class DiskChecker {

    private static final Logger LOG = LoggerFactory.getLogger(DiskChecker.class);
    // 磁盘使用阈值，超过就表示满了
    private float diskUsageThreshold;
    // 磁盘使用率 告警阈值
    private float diskUsageWarnThreshold;

    /**
     * 磁盘相关异常的通用标记。
     * A general marker for disk-related exceptions.
     */
    public abstract static class DiskException extends IOException {
        public DiskException(String msg) {
            super(msg);
        }
    }

    /**
     * 磁盘错误异常。
     * A disk error exception.
     */
    public static class DiskErrorException extends DiskException {
        private static final long serialVersionUID = 9091606022449761729L;

        public DiskErrorException(String msg) {
            super(msg);
        }
    }

    /**
     * 磁盘空间不足异常。
     * An out-of-space disk exception.
     */
    public static class DiskOutOfSpaceException extends DiskException {
        private static final long serialVersionUID = 160898797915906860L;

        private final float usage;

        public DiskOutOfSpaceException(String msg, float usage) {
            super(msg);
            this.usage = usage;
        }

        public float getUsage() {
            return usage;
        }
    }

    /**
     * 磁盘警告阈值异常。
     * A disk warn threshold exception.
     */
    public static class DiskWarnThresholdException extends DiskException {
        private static final long serialVersionUID = -1629284987500841657L;

        private final float usage;

        public DiskWarnThresholdException(String msg, float usage) {
            super(msg);
            this.usage = usage;
        }

        public float getUsage() {
            return usage;
        }
    }

    // 传入磁盘满阈值threshold，默认是95%；磁盘使用率告警阈值warnThreshold，默认是90%
    public DiskChecker(float threshold, float warnThreshold) {
        // 检查磁盘的两个阈值是否合理
        validateThreshold(threshold, warnThreshold);
        // 磁盘使用率
        this.diskUsageThreshold = threshold;
        // 磁盘预警使用率
        this.diskUsageWarnThreshold = warnThreshold;
    }

    /**
     *
     * mkdirsWithExistsCheck 方法的语义与 Sun 的 java.io.File 类中提供的 mkdirs 方法的不同之处如下： 在创建不存在的父目录时，
     * 如果 mkdir 在任何情况下失败，此方法将检查这些目录是否存在 点（因为该目录可能刚刚由其他某个进程创建）。
     * 如果 mkdir() 和 exists() 检查对于任何看似不存在的目录都失败，那么我们会发出错误信号；
     * 如果 Sun 的 mkdir 尝试创建的目录已经存在或 mkdir 失败，则会发出错误信号（返回 false）。
     *
     * The semantics of mkdirsWithExistsCheck method is different from the
     * mkdirs method provided in the Sun's java.io.File class in the following
     * way: While creating the non-existent parent directories, this method
     * checks for the existence of those directories if the mkdir fails at any
     * point (since that directory might have just been created by some other
     * process). If both mkdir() and the exists() check fails for any seemingly
     * non-existent directory, then we signal an error; Sun's mkdir would signal
     * an error (return false) if a directory it is attempting to create already
     * exists or the mkdir fails.
     *
     * @param dir
     * @return true on success, false on failure
     */
    private static boolean mkdirsWithExistsCheck(File dir) {
        // 如果目录创建成功或目录存在，那么直接返回true
        if (dir.mkdir() || dir.exists()) {
            return true;
        }
        // 如果走到这里，那么就是创建目录失败并且目录还不存在，那么有可能是因为给定的文件路径不规范导致的，
        //下面我们就通过getCanonicalFile方法来进行规范化后，再重新执行mkdirsWithExistsCheck。
        //getCanonicalFile方法会将比如：c:\\users\\..\\program这种不规范的路径，规范成"c:\program
        File canonDir = null;
        try {
            canonDir = dir.getCanonicalFile();
        } catch (IOException e) {
            return false;
        }
        String parent = canonDir.getParent();
        return (parent != null)
                && (mkdirsWithExistsCheck(new File(parent)) && (canonDir
                        .mkdir() || canonDir.exists()));
    }

    /**
     * 检查可用磁盘空间。
     * Checks the disk space available.
     *
     * 传入的参数dir是 检查磁盘空间的目录；
     *
     * 如果磁盘空间使用率小于给定的阈值(默认是95%)，那么久会抛DiskOutOfSpaceException
     * @param dir
     *            Directory to check for the disk space
     * @throws DiskOutOfSpaceException
     *             Throws {@link DiskOutOfSpaceException} if available space is
     *             less than threshold.
     */
    @VisibleForTesting
    float checkDiskFull(File dir) throws DiskOutOfSpaceException, DiskWarnThresholdException {
        // 如果给定的目录为null，那么久直接返回0
        if (null == dir) {
            return 0f;
        }
        if (dir.exists()) {
            // 如果给定的目录存在，那么久检查该目录，否则就再次调用该方法，检查上一级目录
            // 获取该目录所在磁盘的剩余空间大小，单位是字节,可用空间大小
            long usableSpace = dir.getUsableSpace();
            // 目录所在磁盘总的空间大小
            long totalSpace = dir.getTotalSpace();
            // 空闲率
            float free = (float) usableSpace / (float) totalSpace;
            // 使用率
            float used = 1f - free;
            // 如果使用率大于磁盘使用率阈值(默认是0.95)，那么就会直接抛异常，并且这种异常我们认为是error级别
            if (used > diskUsageThreshold) {
                LOG.error("Space left on device {} : {}, Used space fraction: {} > threshold {}.",
                        dir, usableSpace, used, diskUsageThreshold);
                throw new DiskOutOfSpaceException("Space left on device "
                        + usableSpace + " Used space fraction:" + used + " > threshold " + diskUsageThreshold, used);
            }
            //如果使用率超过warn级别，默认是0.9,那么也会抛异常，但这种异常我们认为是warn级别
            // Warn should be triggered only if disk usage threshold doesn't trigger first.
            if (used > diskUsageWarnThreshold) {
                LOG.warn("Space left on device {} : {}, Used space fraction: {} > WarnThreshold {}.",
                        dir, usableSpace, used, diskUsageWarnThreshold);
                throw new DiskWarnThresholdException("Space left on device:"
                        + usableSpace + " Used space fraction:" + used + " > WarnThreshold:" + diskUsageWarnThreshold,
                        used);
            }
            //最后返回该目录当前使用率
            return used;
        } else {
            // 递归调用上一级目录
            return checkDiskFull(dir.getParentFile());
        }
    }


    /**
     * 计算放在一起的所有分类帐目录中可用的可用空间总量。
     *
     * Calculate the total amount of free space available
     * in all of the ledger directories put together.
     *
     * @return totalDiskSpace in bytes
     * @throws IOException
     */
    public long getTotalFreeSpace(List<File> dirs) throws IOException {
        long totalFreeSpace = 0;
        Set<FileStore> dirsFileStore = new HashSet<FileStore>();
        // 变能力每个目录
        for (File dir : dirs) {
            // getFileStore应该是根据目录来获取对应的磁盘存储
            FileStore fileStore = Files.getFileStore(dir.toPath());
            // 如果dirsFileStore中不存在fileStore，那么就add成功，并返回true，否则false，
            //这样如果我们在同一块盘配置了多个目录，总的可用磁盘空间容量也不会算重
            if (dirsFileStore.add(fileStore)) {
                totalFreeSpace += fileStore.getUsableSpace();
            }
        }
        return totalFreeSpace;
    }

    /**
     *
     * 计算放在一起的所有分类帐目录中可用的可用空间总量。
     * Calculate the total amount of free space available
     * in all of the ledger directories put together.
     *
     * @return freeDiskSpace in bytes
     * @throws IOException
     */
    public long getTotalDiskSpace(List<File> dirs) throws IOException {
        long totalDiskSpace = 0;
        Set<FileStore> dirsFileStore = new HashSet<FileStore>();
        for (File dir : dirs) {
            // 同样的获取该目录对应的存储对象
            FileStore fileStore = Files.getFileStore(dir.toPath());
            if (dirsFileStore.add(fileStore)) {//如果已经存在了，add会失败，并返回false
                totalDiskSpace += fileStore.getTotalSpace();
            }
        }
        return totalDiskSpace;
    }

    /**
     * 这里其实就是计算目录对应的磁盘，总的使用率
     * calculates and returns the disk usage factor in the provided list of dirs.
     *
     * @param dirs
     *            list of directories
     * @return disk usage factor in the provided list of dirs
     * @throws IOException
     */
    public float getTotalDiskUsage(List<File> dirs) throws IOException {
        if (dirs == null || dirs.isEmpty()) {
            throw new IllegalArgumentException(
                    "list argument of getTotalDiskUsage is not supposed to be null or empty");
        }
        // 总的使用空间大小/总的磁盘空间大小
        float free = (float) getTotalFreeSpace(dirs) / (float) getTotalDiskSpace(dirs);
        float used = 1f - free;
        return used;
    }

    /**
     * 如果目录不存在，则创建该目录。
     *
     * Create the directory if it doesn't exist.
     *
     * dir : 检查磁盘是否满所对应的目录
     *
     * @param dir
     *            Directory to check for the disk error/full.
     * 如果磁盘有错误，就抛DiskErrorException；
     * 如果磁盘的可用空间少于配置的数量，就抛DiskWarnThresholdException；
     * 如果磁盘满了或小于阈值的空间，那么久抛DiskOutOfSpaceException异常
     *
     * @throws DiskErrorException
     *             If disk having errors
     * @throws DiskWarnThresholdException
     *             If disk has less than configured amount of free space.
     * @throws DiskOutOfSpaceException
     *             If disk is full or having less space than threshold
     */
    public float checkDir(File dir) throws DiskErrorException,
            DiskOutOfSpaceException, DiskWarnThresholdException {
        // 检查该目录所在磁盘是否满了，返回的usage是磁盘空间使用率
        float usage = checkDiskFull(dir);
        // 检查是否存在，不存在就创建目录
        if (!mkdirsWithExistsCheck(dir)) {
            throw new DiskErrorException("can not create directory: "
                    + dir.toString());
        }

        // 如果dir不是目录，那么久抛磁盘错误异常
        if (!dir.isDirectory()) {
            throw new DiskErrorException("not a directory: " + dir.toString());
        }

        // 如果目录不可读:这里看起来只是检查下是否有可读权限
        if (!dir.canRead()) {
            throw new DiskErrorException("directory is not readable: "
                    + dir.toString());
        }

        // 检查下是否有可写权限
        if (!dir.canWrite()) {
            throw new DiskErrorException("directory is not writable: "
                    + dir.toString());
        }
        //返回该目录对应磁盘的使用率
        return usage;
    }

    /**
     * 设置磁盘空间阈值。
     * Set the disk space threshold.
     *
     * @param diskSpaceThreshold
     */
    void setDiskSpaceThreshold(float diskSpaceThreshold, float diskUsageWarnThreshold) {
        // 校验阈值是否合法
        validateThreshold(diskSpaceThreshold, diskUsageWarnThreshold);
        this.diskUsageThreshold = diskSpaceThreshold;
        this.diskUsageWarnThreshold = diskUsageWarnThreshold;
    }

    // diskSpaceThreshold应该大于1，并且diskSpaceWarnThreshold应该小于等于diskSpaceThreshold
    private void validateThreshold(float diskSpaceThreshold, float diskSpaceWarnThreshold) {
        if (diskSpaceThreshold <= 0 || diskSpaceThreshold >= 1 || diskSpaceWarnThreshold - diskSpaceThreshold > 1e-6) {
            throw new IllegalArgumentException("Disk space threashold: "
                    + diskSpaceThreshold + " and warn threshold: " + diskSpaceWarnThreshold
                    + " are not valid. Should be > 0 and < 1 and diskSpaceThreshold >= diskSpaceWarnThreshold");
        }
    }

    public float getDiskUsageThreshold() {
        return diskUsageThreshold;
    }

    public float getDiskUsageWarnThreshold() {
        return diskUsageWarnThreshold;
    }
}
