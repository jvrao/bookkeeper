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

package org.apache.bookkeeper.client;

import java.security.GeneralSecurityException;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;

public class WriteLedgerHandle extends LedgerHandle {

	WriteLedgerHandle(BookKeeper bk, long ledgerId, LedgerMetadata metadata,
			DigestType digestType, byte[] password)
			throws GeneralSecurityException, NumberFormatException {
		super(bk, ledgerId, metadata, digestType, password);
	}
	
    /**
     * Add entry synchronously to an open ledger.
     *
     * @param entryId
     *         entryId of the entry to add
     * @param data
     *         array of bytes to be written to the ledger
     * @return 
     */
	@Override
    public long addEntry(final long entryId, byte[] data) throws InterruptedException, BKException {
    	return addEntry(entryId, data, 0, data.length);
    }
    
    /**
     * Add entry synchronously to an open ledger.
     *
     * @param entryId
     *         entryId of the entry to add
     * @param data
     *         array of bytes to be written to the ledger
     * @param offset
     *          offset from which to take bytes from data
     * @param length
     *          number of bytes to take from data
     * @return The entryId of newly inserted entry.
     */
    @Override
    public long addEntry(final long entryId, byte[] data, int offset, int length)
            throws InterruptedException, BKException {
        LOG.debug("Adding entry {}", data);

        SyncCounter counter = new SyncCounter();
        counter.inc();

        SyncAddCallback callback = new SyncAddCallback();
        asyncAddEntry(entryId, data, offset, length, callback, counter);
        counter.block(0);

        if (counter.getrc() != BKException.Code.OK) {
            throw BKException.create(counter.getrc());
        }
        return callback.entryId;
    }
    
    /**
     * Add entry asynchronously to an open ledger, using an offset and range.
     *
     * @param entryId
     *         entryId of the entry to add
     * @param data
     *          array of bytes to be written
     * @param offset
     *          offset from which to take bytes from data
     * @param length
     *          number of bytes to take from data
     * @param cb
     *          object implementing callbackinterface
     * @param ctx
     *          some control object
     * @throws ArrayIndexOutOfBoundsException if offset or length is negative or
     *          offset and length sum to a value higher than the length of data.
     */

    public void asyncAddEntry(final long entryId, final byte[] data, final int offset,
    		final int length, final AddCallback cb, final Object ctx) {
    	PendingAddOp op = new PendingAddOp(this, cb, ctx);
    	op.setEntryId(entryId);
        pendingAddOps.add(op);
    	doAsyncAddEntry(op, data, offset, length, cb, ctx);
    }
	
}
