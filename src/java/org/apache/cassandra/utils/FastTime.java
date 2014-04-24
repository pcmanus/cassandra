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
package org.apache.cassandra.utils;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;

/**
 * Provides the current time with microseconds precision with some reasonable accuracy through
 * the use of clock_gettime (http://man7.org/linux/man-pages/man2/clock_gettime.2.html via JNA).
 *
 * But because calling clock_gettime is slightly expensive, we only call it once per UPDATE_INTERVAL_MS
 * and adds the number of nanoseconds since the last call to get the current time, which is good
 * enough accuracy for our purpose (see #6106).
 */
public class FastTime
{
    private static final Logger logger = LoggerFactory.getLogger(FastTime.class);

    private static final int CLOCK_REALTIME = 0;
    private static final long UPDATE_INTERVAL_MS = 1000;

    private static final TimeLibrary library;
    private static final AtomicReference<TimeSpec> lastFetchedTime;

    static
    {
        TimeLibrary lib = null;
        try
        {
            lib = (TimeLibrary) Native.loadLibrary("rt", TimeLibrary.class);
        }
        catch (Throwable t)
        {
            logger.warn("Could not load accurate time library. This feature is only available on linux.");
        }
        library = lib;

        lastFetchedTime = new AtomicReference<>(TimeSpec.fetch());

        Thread updater = new Thread(new Runnable()
        {
            public void run()
            {
                while (true)
                {
                    try
                    {
                        lastFetchedTime.set(TimeSpec.fetch());
                        Thread.sleep(UPDATE_INTERVAL_MS);
                    }
                    catch (Exception e)
                    {
                        logger.error("Unexpected error while fetching current time", e);
                    }
                }
            }
        }, "Time Updater");
        updater.setDaemon(true);
        updater.start();
    }

    /**
     * @return an approximation of the real-time in microseconds.
     */
    public static long getRealTimeMicros()
    {
        TimeSpec spec = lastFetchedTime.get();
        long curNano = System.nanoTime();
        return spec.timeInMicros + ((curNano - spec.nanoTimeAtCheck) / 1000);
    }

    /**
     * Get the best real time we can. Possibly slightly expensive method.
     */
    private static long getRawRealTimeMicros()
    {
        if (library == null)
        {
            // if no library loaded, fall back to best effort with currentTimeMillis
            return System.currentTimeMillis() * 1000;
        }
        else
        {
            TimeStructure struct = new TimeStructure();
            library.clock_gettime(CLOCK_REALTIME, struct);
            return (struct.tv_sec * 1000000) + (struct.tv_nsec / 1000);
        }
    }

    /**
     * Records a time in micros along with the nanoTime() value at the time the
     * time is fetch.
     */
    private static class TimeSpec
    {
        private final long timeInMicros;
        private final long nanoTimeAtCheck;

        private TimeSpec(long timeInMicros, long nanoTimeAtCheck)
        {
            this.timeInMicros = timeInMicros;
            this.nanoTimeAtCheck = nanoTimeAtCheck;
        }

        static TimeSpec fetch()
        {
            // To compensate for the fact that the getRawRealTimeMicros call could take
            // some time, instead of picking the nano time before the call or after the
            // call, we take the average of both.
            long start = System.nanoTime();
            long micros = getRawRealTimeMicros();
            long end = System.nanoTime();

            // If it turns out the call took us more than 1 milliseconds (can happen while
            // the JVM warms up, unlikely otherwise, but no reasons to take risks), fall back
            // to System.currentMillis() temporarly
            if ((end - start) > 1000000)
                return new TimeSpec(System.currentTimeMillis() * 1000, System.nanoTime());

            return new TimeSpec(micros, (end + start) / 2);
        }
    }

    public interface TimeLibrary extends Library
    {
        int clock_gettime(int clock_id, TimeStructure struct);
    }

    public static class TimeStructure extends Structure
    {
        static final List fieldOrder = Arrays.asList("tv_sec", "tv_nsec");
        public long     tv_sec;        /* seconds */
        public long     tv_nsec;       /* nanoseconds */

        protected List getFieldOrder()
        {
            return fieldOrder;
        }
    }
}
