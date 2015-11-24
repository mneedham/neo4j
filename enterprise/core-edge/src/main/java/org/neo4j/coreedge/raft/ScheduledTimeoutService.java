/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.helpers.Clock;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.lang.System.nanoTime;

import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.POOLED;

public class ScheduledTimeoutService extends LifecycleAdapter implements Runnable, TimeoutService
{
    /**
     * Sorted by next-to-trigger.
     */
    private final SortedSet<ScheduledTimeout> timeouts = new TreeSet<>();
    private final Queue<ScheduledTimeout> pendingRenewals = new ConcurrentLinkedDeque<>();
    private final Clock clock;
    private final Random random;
    private final JobScheduler scheduler;
    private JobScheduler.JobHandle jobHandle;

    public ScheduledTimeoutService()
    {
        this.clock = Clock.SYSTEM_CLOCK;
        this.random = new Random( nanoTime() );
        this.scheduler = new Neo4jJobScheduler();
    }

    /**
     * Set up a new timeout. The attachment is optional data to pass along to the trigger, and can be set to Object
     * and null if you don't care about it.
     * <p/>
     * The randomRange attribute allows you to introduce a bit of arbitrariness in when the timeout is triggered, which
     * is a useful way to avoid "thundering herds" when multiple timeouts are likely to trigger at the same time.
     * <p/>
     * If you don't want randomness, set randomRange to 0.
     */
    @Override
    public Timeout create( TimeoutName name, long milliseconds, long randomRange, TimeoutHandler handler )
    {
        ScheduledTimeout timeout = new ScheduledTimeout(
                calcTimeoutTimestamp( milliseconds, randomRange ),
                milliseconds, randomRange, handler, this );

        synchronized ( timeouts )
        {
            timeouts.add( timeout );
        }

        return timeout;
    }

    public void renew( ScheduledTimeout timeout )
    {
        pendingRenewals.offer( timeout );
    }

    public void cancel( ScheduledTimeout timeout )
    {
        synchronized ( timeouts )
        {
            timeouts.remove( timeout );
        }
    }

    private long calcTimeoutTimestamp( long milliseconds, long randomRange )
    {
        int randomness = randomRange != 0 ? random.nextInt( (int) randomRange ) : 0;
        return clock.currentTimeMillis() + milliseconds + randomness;
    }

    @Override
    public void run()
    {
        try
        {
            long now = clock.currentTimeMillis();
            Collection<ScheduledTimeout> triggered = new LinkedList<>();

            synchronized ( timeouts )
            {
                // Handle renewals
                ScheduledTimeout renew;
                while ( (renew = pendingRenewals.poll()) != null )
                {
                    timeouts.remove( renew );
                    renew.setTimeoutTimestamp( calcTimeoutTimestamp( renew.timeoutLength, renew.randomRange ) );
                    timeouts.add( renew );
                }

                // Trigger timeouts
                for ( ScheduledTimeout timeout : timeouts )
                {
                    if ( timeout.shouldTrigger( now ) )
                    {
                        triggered.add( timeout );
                        timeout.trigger();
                    }
                    else
                    {
                        // Since the timeouts are sorted, the first timeout we hit that should not be triggered means
                        // there are no others that should either, so we bail.
                        break;
                    }
                }

                timeouts.removeAll( triggered );
            }
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
        }
    }

    @Override
    public void init() throws Throwable
    {
        scheduler.init();
    }

    @Override
    public void start() throws Throwable
    {
        jobHandle = scheduler.scheduleRecurring( new JobScheduler.Group( "Scheduler", POOLED ), this, 1,
                TimeUnit.MILLISECONDS );
    }

    @Override
    public void stop() throws Throwable
    {
        jobHandle.cancel( true );
        scheduler.shutdown();
    }

    public static class ScheduledTimeout implements Timeout, Comparable<ScheduledTimeout>
    {
        private static final AtomicLong idGen = new AtomicLong();
        private final long id = idGen.getAndIncrement();
        private final long timeoutLength;
        private final long randomRange;
        private final TimeoutHandler handler;
        private final ScheduledTimeoutService timeouts;
        private long timeoutTimestampMillis;

        public ScheduledTimeout( long timeoutTimestampMillis, long timeoutLength, long randomRange, TimeoutHandler
                handler, ScheduledTimeoutService timeouts )
        {
            this.timeoutTimestampMillis = timeoutTimestampMillis;
            this.timeoutLength = timeoutLength;
            this.randomRange = randomRange;
            this.handler = handler;
            this.timeouts = timeouts;
        }

        private void trigger()
        {
            handler.onTimeout( this );
        }

        private boolean shouldTrigger( long currentTime )
        {
            return currentTime >= timeoutTimestampMillis;
        }

        @Override
        public void renew()
        {
            timeouts.renew( this );
        }

        @Override
        public void cancel()
        {
            timeouts.cancel( this );
        }

        public void setTimeoutTimestamp( long newTimestamp )
        {
            this.timeoutTimestampMillis = newTimestamp;
        }

        @Override
        public int compareTo( ScheduledTimeout o )
        {
            if ( timeoutTimestampMillis == o.timeoutTimestampMillis )
            {
                // Timeouts are set to trigger at the same time.
                // Order them by id instead.
                return (int) (id - o.id);
            }

            return (int) (timeoutTimestampMillis - o.timeoutTimestampMillis);
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            ScheduledTimeout timeout = (ScheduledTimeout) o;

            return id == timeout.id;
        }

        @Override
        public int hashCode()
        {
            return (int) (id ^ (id >>> 32));
        }

        @Override
        public String toString()
        {
            return "Timeout{" +
                    "id=" + id +
                    ", randomRange=" + randomRange +
                    ", timeoutLength=" + timeoutLength +
                    ", timeoutTimestampMillis=" + timeoutTimestampMillis +
                    ", handler=" + handler +
                    '}';
        }
    }
}
