/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.util.collections;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.StampedLock;

/**
 * Concurrent hash map where both keys and values are composed of pairs of longs.
 *
 * <p>(long,long) --&gt; (long,long)
 *
 * <p>Provides similar methods as a {@code ConcurrentMap<K,V>} but since it's an open hash map with linear probing,
 * no node allocations are required to store the keys and values, and no boxing is required.
 *
 * <p>Keys <strong>MUST</strong> be &gt;= 0.
 */
public class ConcurrentLongLongPairHashMap {
    // EmptyKey有什么作用,用来初始化key值
    private static final long EmptyKey = -1L;
    // 如果删除某项，那么会用这个来标记
    private static final long DeletedKey = -2L;
    // 在清理bucket后，会在value值用这个来赋值
    private static final long ValueNotFound = -1L;
    // map填满66就会触发扩容，rehash
    private static final float MapFillFactor = 0.66f;
    //一开始，该map最多可以存放的item，超过就可能触发扩容
    private static final int DefaultExpectedItems = 256;
    //默认的并发数,其实就是section数，分了几段
    private static final int DefaultConcurrencyLevel = 16;

    //分段数据
    private final Section[] sections;

    /**
     * A BiConsumer Long pair.
     */
    public interface BiConsumerLongPair {
        void accept(long key1, long key2, long value1, long value2);
    }

    /**
     * A Long pair function.
     */
    public interface LongLongPairFunction {
        long apply(long key1, long key2);
    }

    /**
     * A Long pair predicate.
     */
    public interface LongLongPairPredicate {
        boolean test(long key1, long key2, long value1, long value2);
    }

    public ConcurrentLongLongPairHashMap() {
        this(DefaultExpectedItems);
    }

    public ConcurrentLongLongPairHashMap(int expectedItems) {
        this(expectedItems, DefaultConcurrencyLevel);
    }

    public ConcurrentLongLongPairHashMap(int expectedItems, int concurrencyLevel) {
        //检验参数是否合法
        checkArgument(expectedItems > 0);
        checkArgument(concurrencyLevel > 0);
        checkArgument(expectedItems >= concurrencyLevel);

        //section数，分段数
        int numSections = concurrencyLevel;
        //每个分段最多包含的item数，超过就要扩容
        int perSectionExpectedItems = expectedItems / numSections;
        //每个分段最多的容量大小
        int perSectionCapacity = (int) (perSectionExpectedItems / MapFillFactor);
        //初始化分段数组
        this.sections = new Section[numSections];

        for (int i = 0; i < numSections; i++) {
            sections[i] = new Section(perSectionCapacity);
        }
    }

    //返回该map实际存放的item数量
    public long size() {
        long size = 0;
        for (Section s : sections) {
            size += s.size;
        }
        return size;
    }

    //返回总的容量(item为单位)
    public long capacity() {
        long capacity = 0;
        for (Section s : sections) {
            capacity += s.capacity;
        }
        return capacity;
    }

    //存放的item是否为空
    public boolean isEmpty() {
        for (Section s : sections) {
            if (s.size != 0) {
                return false;
            }
        }

        return true;
    }

    //获取bucket的数量
    long getUsedBucketCount() {
        long usedBucketCount = 0;
        for (Section s : sections) {
            usedBucketCount += s.usedBuckets;
        }
        return usedBucketCount;
    }

    /**
     * 根据key1和key2获取对应的value1和value2
     * @param key
     * @return the value or -1 if the key was not present
     */
    public LongPair get(long key1, long key2) {
        //为什么只检查key1
        checkBiggerEqualZero(key1);
        //获取到hash值
        long h = hash(key1, key2);
        //根据hash值先找到分段，然后再从该分段中去获取
        return getSection(h).get(key1, key2, (int) h);
    }

    //通过查下来判断是否包含这个key
    public boolean containsKey(long key1, long key2) {
        return get(key1, key2) != null;
    }

    //默认是覆盖写
    public boolean put(long key1, long key2, long value1, long value2) {
        checkBiggerEqualZero(key1);
        checkBiggerEqualZero(value1);
        long h = hash(key1, key2);
        return getSection(h).put(key1, key2, value1, value2, (int) h, false);
    }

    //不覆盖写
    public boolean putIfAbsent(long key1, long key2, long value1, long value2) {
        checkBiggerEqualZero(key1);
        checkBiggerEqualZero(value1);
        long h = hash(key1, key2);
        return getSection(h).put(key1, key2, value1, value2, (int) h, true);
    }

    /**
     * Remove an existing entry if found.
     *
     * @param key
     * @return the value associated with the key or -1 if key was not present
     */
    public boolean remove(long key1, long key2) {
        //为啥这里只检查key1，key2可以不大于等于0吗
        //根据key1和key2来移除
        checkBiggerEqualZero(key1);
        long h = hash(key1, key2);
        return getSection(h).remove(key1, key2, ValueNotFound, ValueNotFound, (int) h);
    }

    public boolean remove(long key1, long key2, long value1, long value2) {
        checkBiggerEqualZero(key1);
        checkBiggerEqualZero(value1);
        long h = hash(key1, key2);
        //根据key和value一起来移除
        return getSection(h).remove(key1, key2, value1, value2, (int) h);
    }

    private Section getSection(long hash) {
        // Use 32 msb out of long to get the section
        final int sectionIdx = (int) (hash >>> 32) & (sections.length - 1);
        return sections[sectionIdx];
    }

    public void clear() {
        for (Section s : sections) {
            s.clear();
        }
    }

    public void forEach(BiConsumerLongPair processor) {
        for (Section s : sections) {
            s.forEach(processor);
        }
    }

    /**
     * @return a new list of all keys (makes a copy)
     */
    public List<LongPair> keys() {
        List<LongPair> keys = Lists.newArrayList();
        forEach((key1, key2, value1, value2) -> keys.add(new LongPair(key1, key2)));
        return keys;
    }

    public List<LongPair> values() {
        List<LongPair> values = Lists.newArrayList();
        forEach((key1, key2, value1, value2) -> values.add(new LongPair(value1, value2)));
        return values;
    }

    public Map<LongPair, LongPair> asMap() {
        Map<LongPair, LongPair> map = Maps.newHashMap();
        forEach((key1, key2, value1, value2) -> map.put(new LongPair(key1, key2), new LongPair(value1, value2)));
        return map;
    }

    // A section is a portion of the hash map that is covered by a single
    @SuppressWarnings("serial")
    private static final class Section extends StampedLock {
        // 键和值交错存储在表数组中
        // Keys and values are stored interleaved in the table array
        private volatile long[] table;
        //这个代表该分段的容量，这个容量不是说table数组的总大小，而是有多少个item，一个item是连续存放的4个元素key1 key2 value1 value2
        private volatile int capacity;
        //实际存放的item数量(一个item包含4个数字)
        private volatile int size;
        private int usedBuckets;
        private int resizeThreshold;

        Section(int capacity) {
            //把奇数向上转成偶数
            this.capacity = alignToPowerOfTwo(capacity);
            //为啥这里要扩大4倍呢?因为有4个元素（key1,key2)->(value1,value2），连续挨着存放，而一对（key1,key2)->(value1,value2）这里看成是一项
            this.table = new long[4 * this.capacity];
            //该分段存储的item数量
            this.size = 0;
            //使用了多少个bucket  这个和size的区别是啥
            this.usedBuckets = 0;
            this.resizeThreshold = (int) (this.capacity * MapFillFactor);
            Arrays.fill(table, EmptyKey);
        }

        LongPair get(long key1, long key2, int keyHash) {
            //先加读锁
            long stamp = tryOptimisticRead();
            //这个拿来干啥的
            boolean acquiredLock = false;
            //获取到对应的bucket索引
            int bucket = signSafeMod(keyHash, capacity);

            //从bucket索引往下找
            try {
                while (true) {
                    // First try optimistic locking
                    long storedKey1 = table[bucket];
                    long storedKey2 = table[bucket + 1];
                    long storedValue1 = table[bucket + 2];
                    long storedValue2 = table[bucket + 3];

                    if (!acquiredLock && validate(stamp)) {
                        // The values we have read are consistent
                        if (key1 == storedKey1 && key2 == storedKey2) {
                            return new LongPair(storedValue1, storedValue2);
                        } else if (storedKey1 == EmptyKey) {
                            // Not found
                            return null;
                        }
                    } else {
                        // Fallback to acquiring read lock
                        if (!acquiredLock) {
                            stamp = readLock();
                            acquiredLock = true;

                            bucket = signSafeMod(keyHash, capacity);
                            storedKey1 = table[bucket];
                            storedKey2 = table[bucket + 1];
                            storedValue1 = table[bucket + 2];
                            storedValue2 = table[bucket + 3];
                        }

                        if (key1 == storedKey1 && key2 == storedKey2) {
                            return new LongPair(storedValue1, storedValue2);
                        } else if (storedKey1 == EmptyKey) {
                            // Not found
                            return null;
                        }
                    }

                    bucket = (bucket + 4) & (table.length - 1);
                }
            } finally {
                if (acquiredLock) {
                    unlockRead(stamp);
                }
            }
        }

        //(key1,key2) -> (value1,value2)
        //keyHash：是通过key1和key2得到的hash值
        boolean put(long key1, long key2, long value1, long value2, int keyHash, boolean onlyIfAbsent) {
            //持有独占锁
            long stamp = writeLock();
            //这里应该是通过hash值来定位到该key1和key2所在的位置
            int bucket = signSafeMod(keyHash, capacity);

            // 记住我们在哪里找到第一个可用位置
            // Remember where we find the first available spot
            int firstDeletedKey = -1;

            try {
                while (true) {
                    long storedKey1 = table[bucket];
                    long storedKey2 = table[bucket + 1];

                    if (key1 == storedKey1 && key2 == storedKey2) {
                        //正常情况下key1和key2这里对应的是leadgerId和entryId，这两个是不可能重复的
                        if (!onlyIfAbsent) {
                            //如果为true，表示没有包含这些key的时候，才赋值，包含的话，就放入失败，返回false；总的来说就是要不要覆盖写
                            // Over written an old value for same key
                            table[bucket + 2] = value1;
                            table[bucket + 3] = value2;
                            return true;
                        } else {
                            return false;
                        }
                    } else if (storedKey1 == EmptyKey) {
                        // 找到一个空桶。 这意味着密钥不在地图中。 如果我们已经看到一个已删除的键，我们应该在那个位置写
                        // Found an empty bucket. This means the key is not in the map. If we've already seen a deleted
                        // key, we should write at that position
                        if (firstDeletedKey != -1) {
                            bucket = firstDeletedKey;
                        } else {
                            //第一次hash，就hash到空bucket的时候，usedBuckets就自增
                            ++usedBuckets;
                        }

                        table[bucket] = key1;
                        table[bucket + 1] = key2;
                        table[bucket + 2] = value1;
                        table[bucket + 3] = value2;
                        ++size;
                        return true;
                    } else if (storedKey1 == DeletedKey) {
                        // The bucket contained a different deleted key
                        if (firstDeletedKey == -1) {
                            firstDeletedKey = bucket;
                        }
                    }

                    bucket = (bucket + 4) & (table.length - 1);
                }
            } finally {
                if (usedBuckets > resizeThreshold) {
                    //如果超过阈值就会扩容 rehash
                    try {
                        rehash();
                    } finally {
                        unlockWrite(stamp);
                    }
                } else {
                    unlockWrite(stamp);
                }
            }
        }

        private boolean remove(long key1, long key2, long value1, long value2, int keyHash) {
            //加独占锁
            long stamp = writeLock();
            //映射到桶
            int bucket = signSafeMod(keyHash, capacity);

            try {
                while (true) {//注意这里是死循环
                    long storedKey1 = table[bucket];
                    long storedKey2 = table[bucket + 1];
                    long storedValue1 = table[bucket + 2];
                    long storedValue2 = table[bucket + 3];
                    if (key1 == storedKey1 && key2 == storedKey2) {
                        //如果一下就找到，或对应value是ValueNotFound，那么size减少1，然后清理bucket
                        if (value1 == ValueNotFound || (value1 == storedValue1 && value2 == storedValue2)) {
                            --size;
                            //清理bucket
                            cleanBucket(bucket);
                            //到此该item就移除完毕，返回true
                            return true;
                        } else {
                            //否则就是移除失败
                            return false;
                        }
                    } else if (storedKey1 == EmptyKey) {
                        // 如果找到的key是EmptyKey，那么久不继续往下找了，直接返回false
                        // Key wasn't found
                        return false;
                    }

                    //找下一个bucket
                    bucket = (bucket + 4) & (table.length - 1);
                }

            } finally {
                unlockWrite(stamp);
            }
        }

        private void cleanBucket(int bucket) {
            //该bucket结束位置的下一个位置
            int nextInArray = (bucket + 4) & (table.length - 1);
            if (table[nextInArray] == EmptyKey) {
                //如果下一个bucket位置是空，那么该bucket用EmptyKey和ValueNotFound来填充
                //因为在查找的时候，是以EmptyKey
                table[bucket] = EmptyKey;
                table[bucket + 1] = EmptyKey;
                table[bucket + 2] = ValueNotFound;
                table[bucket + 3] = ValueNotFound;
                --usedBuckets;
            } else {
                //如果下一个bucket位置不是空，那么当前的bucket肯定是用过了的？所以当前的bucket需要用DeletedKey和ValueNotFound填充
                table[bucket] = DeletedKey;
                table[bucket + 1] = DeletedKey;
                table[bucket + 2] = ValueNotFound;
                table[bucket + 3] = ValueNotFound;
            }
        }

        void clear() {
            long stamp = writeLock();

            try {
                Arrays.fill(table, EmptyKey);
                this.size = 0;
                this.usedBuckets = 0;
            } finally {
                unlockWrite(stamp);
            }
        }

        public void forEach(BiConsumerLongPair processor) {
            long stamp = tryOptimisticRead();

            long[] table = this.table;
            boolean acquiredReadLock = false;

            try {

                // Validate no rehashing
                if (!validate(stamp)) {
                    // Fallback to read lock
                    stamp = readLock();
                    acquiredReadLock = true;
                    table = this.table;
                }

                // Go through all the buckets for this section
                for (int bucket = 0; bucket < table.length; bucket += 4) {
                    long storedKey1 = table[bucket];
                    long storedKey2 = table[bucket + 1];
                    long storedValue1 = table[bucket + 2];
                    long storedValue2 = table[bucket + 3];

                    if (!acquiredReadLock && !validate(stamp)) {
                        // Fallback to acquiring read lock
                        stamp = readLock();
                        acquiredReadLock = true;

                        storedKey1 = table[bucket];
                        storedKey2 = table[bucket + 1];
                        storedValue1 = table[bucket + 2];
                        storedValue2 = table[bucket + 3];
                    }

                    if (storedKey1 != DeletedKey && storedKey1 != EmptyKey) {
                        processor.accept(storedKey1, storedKey2, storedValue1, storedValue2);
                    }
                }
            } finally {
                if (acquiredReadLock) {
                    unlockRead(stamp);
                }
            }
        }

        private void rehash() {
            // 以2倍的方式扩大
            // Expand the hashmap
            int newCapacity = capacity * 2;
            //新的table表
            long[] newTable = new long[4 * newCapacity];
            //以EmptyKey填充
            Arrays.fill(newTable, EmptyKey);

            // Re-hash table
            for (int i = 0; i < table.length; i += 4) {
                //获取key和value
                long storedKey1 = table[i];
                long storedKey2 = table[i + 1];
                long storedValue1 = table[i + 2];
                long storedValue2 = table[i + 3];
                if (storedKey1 != EmptyKey && storedKey1 != DeletedKey) {
                    //如果key1是真实的值
                    insertKeyValueNoLock(newTable, newCapacity, storedKey1, storedKey2, storedValue1, storedValue2);
                }
            }

            //新的table数组表
            table = newTable;
            //更新使用了的buket数
            usedBuckets = size;
            // Capacity needs to be updated after the values, so that we won't see
            // a capacity value bigger than the actual array size
            capacity = newCapacity;
            //更新扩容阈值
            resizeThreshold = (int) (capacity * MapFillFactor);
        }

        private static void insertKeyValueNoLock(long[] table, int capacity, long key1, long key2, long value1,
                long value2) {
            int bucket = signSafeMod(hash(key1, key2), capacity);

            while (true) {
                long storedKey1 = table[bucket];

                if (storedKey1 == EmptyKey) {
                    // 一定要找到一个来插进去
                    // The bucket is empty, so we can use it
                    table[bucket] = key1;
                    table[bucket + 1] = key2;
                    table[bucket + 2] = value1;
                    table[bucket + 3] = value2;
                    return;
                }

                //往下找下一个bucket
                bucket = (bucket + 4) & (table.length - 1);
            }
        }
    }

    private static final long HashMixer = 0xc6a4a7935bd1e995L;
    private static final int R = 47;

    static final long hash(long key1, long key2) {
        long hash = key1 * HashMixer;
        hash ^= hash >>> R;
        hash *= HashMixer;
        hash += 31 + (key2 * HashMixer);
        hash ^= hash >>> R;
        hash *= HashMixer;
        return hash;
    }

    static final int signSafeMod(long n, int max) {
        return (int) (n & (max - 1)) << 2;
    }
    //把奇数向上转成偶数，比如：7就变成8，5变成6，4就是4
    private static int alignToPowerOfTwo(int n) {
        return (int) Math.pow(2, 32 - Integer.numberOfLeadingZeros(n - 1));
    }

    private static void checkBiggerEqualZero(long n) {
        if (n < 0L) {
            throw new IllegalArgumentException("Keys and values must be >= 0");
        }
    }

    /**
     * A pair of long values.
     */
    public static class LongPair implements Comparable<LongPair> {
        public final long first;
        public final long second;

        public LongPair(long first, long second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof LongPair) {
                LongPair other = (LongPair) obj;
                return first == other.first && second == other.second;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return (int) hash(first, second);
        }

        @Override
        public int compareTo(LongPair o) {
            if (first != o.first) {
                return Long.compare(first, o.first);
            } else {
                return Long.compare(second, o.second);
            }
        }
    }
}
