/*
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
package org.apache.cassandra.db;

import java.util.Objects;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Stores the information relating to the liveness of the primary key columns of a row.
 * <p>
 * A {@code LivenessInfo} can first be empty. If it isn't, it contains at least a timestamp,
 * which is the timestamp for the row primary key columns. On top of that, the info can be
 * ttl'ed, in which case the {@code LivenessInfo} also has both a ttl and a local expiration time.
 */
public class LivenessInfo
{
    public static final long NO_TIMESTAMP = Long.MIN_VALUE;
    public static final int NO_TTL = 0;
    public static final int NO_PURGING_TIME = Integer.MAX_VALUE;

    public static final LivenessInfo EMPTY = new LivenessInfo(NO_TIMESTAMP);

    protected final long timestamp;

    protected LivenessInfo(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public static LivenessInfo create(CFMetaData metadata, long timestamp, int nowInSec)
    {
        int defaultTTL = metadata.params.defaultTimeToLive;
        if (defaultTTL != NO_TTL)
            return expiring(timestamp, defaultTTL, nowInSec);

        return new LivenessInfo(timestamp);
    }

    public static LivenessInfo expiring(long timestamp, int ttl, int nowInSec)
    {
        return new ExpiringLivenessInfo(timestamp, ttl, nowInSec);
    }

    public static LivenessInfo create(CFMetaData metadata, long timestamp, int ttl, int nowInSec)
    {
        return ttl == NO_TTL
             ? create(metadata, timestamp, nowInSec)
             : expiring(timestamp, ttl, nowInSec);
    }

    // Note that this ctor ignores the default table ttl and takes the purging reference time, not the current time.
    // Use when you know that's what you want.
    public static LivenessInfo create(long timestamp, int ttl, int purgingReferenceTime)
    {
        return ttl == NO_TTL ? new LivenessInfo(timestamp) : new ExpiringLivenessInfo(timestamp, ttl, purgingReferenceTime);
    }

    /**
     * Whether this liveness info is empty (has no timestamp).
     *
     * @return whether this liveness info is empty or not.
     */
    public boolean isEmpty()
    {
        return timestamp == NO_TIMESTAMP;
    }

    /**
     * The timestamp for this liveness info.
     *
     * @return the liveness info timestamp (or {@link #NO_TIMESTAMP} if the info is empty).
     */
    public long timestamp()
    {
        return timestamp;
    }

    /**
     * Whether the info has a ttl.
     */
    public boolean isExpiring()
    {
        return false;
    }

    /**
     * The ttl (if any) on the row primary key columns or {@link #NO_TTL} if it is not
     * expiring.
     *
     * Please note that this value is the TTL that was set originally and is thus not
     * changing.
     */
    public int ttl()
    {
        return NO_TTL;
    }

    /**
     * The local time (in seconds) from which gc grace should count for purging if the info is expiring
     * (or {@link NO_PURGING_TIME} if not expiring).
     */
    public int purgingReferenceTime()
    {
        return NO_PURGING_TIME;
    }

    /**
     * Whether that info is still live.
     *
     * A {@code LivenessInfo} is live if it is either not expiring, or if its expiration time if after
     * {@code nowInSec}.
     *
     * @param nowInSec the current time in seconds.
     * @return whether this liveness info is live or not.
     */
    public boolean isLive(int nowInSec)
    {
        return !isEmpty();
    }

    /**
     * Adds this liveness information to the provided digest.
     *
     * @param digest the digest to add this liveness information to.
     */
    public void digest(MessageDigest digest)
    {
        FBUtilities.updateWithLong(digest, timestamp());
    }

    /**
     * Validate the data contained by this liveness information.
     *
     * @throws MarshalException if some of the data is corrupted.
     */
    public void validate()
    {
    }

    /**
     * The size of the (useful) data this liveness information contains.
     *
     * @return the size of the data this liveness information contains.
     */
    public int dataSize()
    {
        return TypeSizes.sizeof(timestamp());
    }

    /**
     * Whether this liveness information supersedes another one (that is
     * whether is has a greater timestamp than the other or not).
     *
     * @param other the {@code LivenessInfo} to compare this info to.
     *
     * @return whether this {@code LivenessInfo} supersedes {@code other}.
     */
    public boolean supersedes(LivenessInfo other)
    {
        return timestamp > other.timestamp;
    }

    /**
     * Returns a copy of this liveness info updated with the provided timestamp.
     *
     * @param newTimestamp the timestamp for the returned info.
     * @return if this liveness info has a timestamp, a copy of it with {@code newTimestamp}
     * as timestamp. If it has no timestamp however, this liveness info is returned
     * unchanged.
     */
    public LivenessInfo withUpdatedTimestamp(long newTimestamp)
    {
        return new LivenessInfo(newTimestamp);
    }

    @Override
    public String toString()
    {
        return String.format("[ts=%d]", timestamp);
    }

    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof LivenessInfo))
            return false;

        LivenessInfo that = (LivenessInfo)other;
        return this.timestamp() == that.timestamp()
            && this.ttl() == that.ttl()
            && this.purgingReferenceTime() == that.purgingReferenceTime();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(timestamp(), ttl(), purgingReferenceTime());
    }

    private static class ExpiringLivenessInfo extends LivenessInfo
    {
        private final int ttl;
        private final int purgingReferenceTime;

        private ExpiringLivenessInfo(long timestamp, int ttl, int purgingReferenceTime)
        {
            super(timestamp);
            assert ttl != NO_TTL && purgingReferenceTime != NO_PURGING_TIME;
            this.ttl = ttl;
            this.purgingReferenceTime = purgingReferenceTime;
        }

        @Override
        public int ttl()
        {
            return ttl;
        }

        @Override
        public int purgingReferenceTime()
        {
            return purgingReferenceTime;
        }

        @Override
        public boolean isExpiring()
        {
            return true;
        }

        @Override
        public boolean isLive(int nowInSec)
        {
            return nowInSec < purgingReferenceTime + ttl;
        }

        @Override
        public void digest(MessageDigest digest)
        {
            super.digest(digest);
            // We used to digest with the expiration time, which is purgingReferenceTime + ttl, so to avoid
            // create false digest mismatches, we continue to do so.
            FBUtilities.updateWithInt(digest, purgingReferenceTime + ttl);
            FBUtilities.updateWithInt(digest, ttl);
        }

        @Override
        public void validate()
        {
            if (ttl < 0)
                throw new MarshalException("A TTL should not be negative");
            if (purgingReferenceTime < 0)
                throw new MarshalException("A purging reference time should not be negative");
        }

        @Override
        public int dataSize()
        {
            return super.dataSize()
                 + TypeSizes.sizeof(ttl)
                 + TypeSizes.sizeof(purgingReferenceTime);

        }

        @Override
        public LivenessInfo withUpdatedTimestamp(long newTimestamp)
        {
            return new ExpiringLivenessInfo(newTimestamp, ttl, purgingReferenceTime);
        }

        @Override
        public String toString()
        {
            return String.format("[ts=%d ttl=%d, prt=%d]", timestamp, ttl, purgingReferenceTime);
        }
    }
}
