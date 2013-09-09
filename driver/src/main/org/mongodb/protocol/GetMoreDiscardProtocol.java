/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.protocol;

import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ConnectionReceiveArgs;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.SingleResultFuture;

public class GetMoreDiscardProtocol implements Protocol<Void> {
    private final long cursorId;
    private final int responseTo;
    private final Connection connection;

    public GetMoreDiscardProtocol(final long cursorId, final int responseTo, final Connection connection) {
        this.cursorId = cursorId;
        this.responseTo = responseTo;
        this.connection = connection;
    }

    public Void execute() {
        long curCursorId = cursorId;
        int curResponseTo = responseTo;
        while (curCursorId != 0) {
            final ResponseBuffers responseBuffers = connection.receiveMessage(new ConnectionReceiveArgs(curResponseTo));
            try {
                curCursorId = responseBuffers.getReplyHeader().getCursorId();
                curResponseTo = responseBuffers.getReplyHeader().getRequestId();
            } finally {
                responseBuffers.close();
            }
        }
        return null;
    }

    public MongoFuture<Void> executeAsync() {
        final SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();

        if (cursorId == 0) {
            retVal.init(null, null);
        }
        else {
            connection.receiveMessageAsync(new ConnectionReceiveArgs(responseTo), new DiscardCallback(retVal));
        }

        return retVal;

    }

    private class DiscardCallback implements SingleResultCallback<ResponseBuffers> {
        private SingleResultFuture<Void> future;

        public DiscardCallback(final SingleResultFuture<Void> future) {
            this.future = future;
        }

        @Override
        public void onResult(final ResponseBuffers result, final MongoException e) {
            if (result.getReplyHeader().getCursorId() == 0) {
                future.init(null, null);
            }
            else {
                connection.receiveMessageAsync(new ConnectionReceiveArgs(responseTo), this);
            }
        }
    }

}