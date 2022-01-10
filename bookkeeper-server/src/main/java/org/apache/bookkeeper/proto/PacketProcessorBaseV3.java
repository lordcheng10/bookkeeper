/**
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
package org.apache.bookkeeper.proto;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.util.StringUtils;

/**
 * A base class for bookkeeper protocol v3 packet processors.
 */
public abstract class PacketProcessorBaseV3 extends SafeRunnable {

    final Request request;
    final Channel channel;
    final BookieRequestProcessor requestProcessor;
    final long enqueueNanos;

    public PacketProcessorBaseV3(Request request, Channel channel,
                                 BookieRequestProcessor requestProcessor) {
        this.request = request;
        this.channel = channel;
        this.requestProcessor = requestProcessor;
        this.enqueueNanos = MathUtils.nowInNano();
    }

    protected void sendResponse(StatusCode code, Object response, OpStatsLogger statsLogger) {
        final long writeNanos = MathUtils.nowInNano();
        //默认waitTimeoutOnResponseBackpressureMs为-1
        final long timeOut = requestProcessor.getWaitTimeoutOnBackpressureMillis();
        if (timeOut >= 0 && !channel.isWritable()) {
            //看该客户端是否是在黑名单内，如果不是才进入if
            if (!requestProcessor.isBlacklisted(channel)) {
                synchronized (channel) {
                    if (!channel.isWritable() && !requestProcessor.isBlacklisted(channel)) {
                        //如果该channel不是可写，并且该channel没有在黑名单内,那么久等待一段时间timeOut
                        final long waitUntilNanos = writeNanos + TimeUnit.MILLISECONDS.toNanos(timeOut);
                        while (!channel.isWritable() && MathUtils.nowInNano() < waitUntilNanos) {
                            try {
                                TimeUnit.MILLISECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                break;
                            }
                        }
                        if (!channel.isWritable()) {
                            //如果是不可写的，那么久会放入黑名单，下次恢复的时候，会等待一段时间
                            requestProcessor.blacklistChannel(channel);
                            //并且记录到不可写集合中
                            requestProcessor.handleNonWritableChannel(channel);
                        }
                    }
                }
            }

            if (!channel.isWritable()) {
                //如果是不可写的,就会统计下不可写耗时
                LOGGER.warn("cannot write response to non-writable channel {} for request {}", channel,
                        StringUtils.requestToString(request));
                requestProcessor.getRequestStats().getChannelWriteStats()
                        .registerFailedEvent(MathUtils.elapsedNanos(writeNanos), TimeUnit.NANOSECONDS);
                statsLogger.registerFailedEvent(MathUtils.elapsedNanos(enqueueNanos), TimeUnit.NANOSECONDS);
                //这里就直接返回，因为是不可写的channel，那么回复肯定会失败，所以就不用回复了
                return;
            } else {
                //如果是可写的，那么就从黑名单中去掉该channel
                requestProcessor.invalidateBlacklist(channel);
            }
        }

        //这里才是真正回复response
        channel.writeAndFlush(response).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                //这里统计了单纯的回复response耗时
                long writeElapsedNanos = MathUtils.elapsedNanos(writeNanos);
                if (!future.isSuccess()) {
                    //如果是不成功，那么久统计不成功耗时
                    requestProcessor.getRequestStats().getChannelWriteStats()
                        .registerFailedEvent(writeElapsedNanos, TimeUnit.NANOSECONDS);
                } else {
                    //陈宫了就统计成功耗时
                    requestProcessor.getRequestStats().getChannelWriteStats()
                        .registerSuccessfulEvent(writeElapsedNanos, TimeUnit.NANOSECONDS);
                }
                //这里统计整个写请求处理耗时,对应的是ADD_ENTRY_REQUEST
                if (StatusCode.EOK == code) {
                    statsLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(enqueueNanos), TimeUnit.NANOSECONDS);
                } else {
                    statsLogger.registerFailedEvent(MathUtils.elapsedNanos(enqueueNanos), TimeUnit.NANOSECONDS);
                }
            }
        });
    }

    protected boolean isVersionCompatible() {
        return this.request.getHeader().getVersion().equals(ProtocolVersion.VERSION_THREE);
    }

    /**
     * Build a header with protocol version 3 and the operation type same as what was in the
     * request.
     * @return
     */
    protected BKPacketHeader getHeader() {
        BKPacketHeader.Builder header = BKPacketHeader.newBuilder();
        header.setVersion(ProtocolVersion.VERSION_THREE);
        header.setOperation(request.getHeader().getOperation());
        header.setTxnId(request.getHeader().getTxnId());
        return header.build();
    }

    @Override
    public String toString() {
        return request.toString();
    }
}
