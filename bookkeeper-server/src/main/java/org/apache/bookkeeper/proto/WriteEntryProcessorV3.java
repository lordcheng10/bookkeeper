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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieException.OperationRejectedException;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WriteEntryProcessorV3 extends PacketProcessorBaseV3 {
    private static final Logger logger = LoggerFactory.getLogger(WriteEntryProcessorV3.class);

    public WriteEntryProcessorV3(Request request, Channel channel,
                                 BookieRequestProcessor requestProcessor) {
        super(request, channel, requestProcessor);
        requestProcessor.onAddRequestStart(channel);
    }

    // Returns null if there is no exception thrown
    private AddResponse getAddResponse() {
        //记录开始时间
        final long startTimeNanos = MathUtils.nowInNano();
        //add请求
        AddRequest addRequest = request.getAddRequest();
        //这里主要是在构建AddResponseBuilder
        long ledgerId = addRequest.getLedgerId();
        long entryId = addRequest.getEntryId();
        final AddResponse.Builder addResponse = AddResponse.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);

        //判断版本是否兼容,看起来bookkeeper没有完全的向下兼容
        if (!isVersionCompatible()) {
            //如果不兼容，就直接返回EBADVERSION错误码
            addResponse.setStatus(StatusCode.EBADVERSION);
            return addResponse.build();
        }

        //如果该bookie节点是只读的并且该请求不是高优的写的，或当前bookie不是提供高优写的，那么也会以EREADONLY的错误码直接返回
        if (requestProcessor.getBookie().isReadOnly()
            && !(RequestUtils.isHighPriority(request)
                    && requestProcessor.getBookie().isAvailableForHighPriorityWrites())) {
            logger.warn("BookieServer is running as readonly mode, so rejecting the request from the client!");
            addResponse.setStatus(StatusCode.EREADONLY);
            return addResponse.build();
        }

        //构建response请求的回调方法
        BookkeeperInternalCallbacks.WriteCallback wcb = new BookkeeperInternalCallbacks.WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId,
                                      BookieId addr, Object ctx) {
                if (BookieProtocol.EOK == rc) {
                    //如果成功了,那么统计下耗时,这里只是统计了写成功耗时，还未包含回复response的耗时
                    requestProcessor.getRequestStats().getAddEntryStats()
                        .registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
                } else {
                    //这里失败了，也会统计下耗时,这里的成功和失败，会通过其中的一个success标签来标记
                    requestProcessor.getRequestStats().getAddEntryStats()
                        .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
                }

                //计算回复response的错误码
                StatusCode status;
                switch (rc) {
                    case BookieProtocol.EOK:
                        status = StatusCode.EOK;
                        break;
                    case BookieProtocol.EIO:
                        status = StatusCode.EIO;
                        break;
                    default:
                        status = StatusCode.EUA;
                        break;
                }
                addResponse.setStatus(status);
                //构建response
                Response.Builder response = Response.newBuilder()
                        .setHeader(getHeader())
                        .setStatus(addResponse.getStatus())
                        .setAddResponse(addResponse);
                Response resp = response.build();
                //回复response
                sendResponse(status, resp, requestProcessor.getRequestStats().getAddRequestStats());
            }
        };


        //判断是否有写flag
        final EnumSet<WriteFlag> writeFlags;
        if (addRequest.hasWriteFlags()) {
            writeFlags = WriteFlag.getWriteFlags(addRequest.getWriteFlags());
        } else {
            writeFlags = WriteFlag.NONE;
        }
        //在同步之前ack
        final boolean ackBeforeSync = writeFlags.contains(WriteFlag.DEFERRED_SYNC);
        StatusCode status = null;
        //获取key，整个key是啥
        byte[] masterKey = addRequest.getMasterKey().toByteArray();
        //这里获取要写入的数据
        ByteBuf entryToAdd = Unpooled.wrappedBuffer(addRequest.getBody().asReadOnlyByteBuffer());
        try {
            //看看是否是recovery的写
            if (RequestUtils.hasFlag(addRequest, AddRequest.Flag.RECOVERY_ADD)) {
                requestProcessor.getBookie().recoveryAddEntry(entryToAdd, wcb, channel, masterKey);
            } else {
                //不是recovery的写，就是客户端的写
                requestProcessor.getBookie().addEntry(entryToAdd, ackBeforeSync, wcb, channel, masterKey);
            }
            status = StatusCode.EOK;
        } catch (OperationRejectedException e) {
            // Avoid to log each occurence of this exception as this can happen when the ledger storage is
            // unable to keep up with the write rate.
            if (logger.isDebugEnabled()) {
                logger.debug("Operation rejected while writing {}", request, e);
            }
            status = StatusCode.EIO;
        } catch (IOException e) {
            logger.error("Error writing entry:{} to ledger:{}",
                    entryId, ledgerId, e);
            status = StatusCode.EIO;
        } catch (BookieException.LedgerFencedException e) {
            logger.error("Ledger fenced while writing entry:{} to ledger:{}",
                    entryId, ledgerId, e);
            status = StatusCode.EFENCED;
        } catch (BookieException e) {
            logger.error("Unauthorized access to ledger:{} while writing entry:{}",
                    ledgerId, entryId, e);
            status = StatusCode.EUA;
        } catch (Throwable t) {
            logger.error("Unexpected exception while writing {}@{} : ",
                    entryId, ledgerId, t);
            // some bad request which cause unexpected exception
            status = StatusCode.EBADREQ;
        }

        //如果错误码不为OK，那么久设置好错误码，然后返回response
        // If everything is okay, we return null so that the calling function
        // doesn't return a response back to the caller.
        if (!status.equals(StatusCode.EOK)) {
            addResponse.setStatus(status);
            return addResponse.build();
        }
        //如果都是OK的，那么久返回null
        return null;
    }

    @Override
    public void safeRun() {
        AddResponse addResponse = getAddResponse();
        if (null != addResponse) {
            //如果不为null，那么久说明是有错误的，那么这里回构建response进行发送.
            //因为在写完成的回调里面，没有对有异常的写进行处理
            // This means there was an error and we should send this back.
            Response.Builder response = Response.newBuilder()
                    .setHeader(getHeader())
                    .setStatus(addResponse.getStatus())
                    .setAddResponse(addResponse);
            Response resp = response.build();
            sendResponse(addResponse.getStatus(), resp,
                         requestProcessor.getRequestStats().getAddRequestStats());
        }
    }

    @Override
    protected void sendResponse(StatusCode code, Object response, OpStatsLogger statsLogger) {
        //先回复response
        super.sendResponse(code, response, statsLogger);
        //释放信号量和正在处理的add请求书减1
        requestProcessor.onAddRequestFinish();
    }

    /**
     * this toString method filters out body and masterKey from the output.
     * masterKey contains the password of the ledger and body is customer data,
     * so it is not appropriate to have these in logs or system output.
     */
    @Override
    public String toString() {
        return RequestUtils.toSafeString(request);
    }
}
