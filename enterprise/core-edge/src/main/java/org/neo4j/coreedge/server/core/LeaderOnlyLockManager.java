/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.server.core;

import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.server.core.CurrentReplicatedLockState.LockSession;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.locking.AcquireLockTimeoutException;
import org.neo4j.kernel.impl.locking.LockClientAlreadyClosedException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;

/**
 * Each member of the cluster uses its own {@link LeaderOnlyLockManager} which wraps a local {@link Locks}.
 * To prevent conflict between the local {@link Locks}, before issuing any locks, each server must first obtain a
 * replicated exclusive lock via {@link ReplicatedLockStateMachine}.
 *
 * Since replication is only allowed from the leader, this means that only the leader is able to obtain
 * a replicated lock, and therefore only the leader can issue locks.
 */
public class LeaderOnlyLockManager<MEMBER> implements Locks
{
    public static final int LOCK_WAIT_TIME = 30000;

    private final MEMBER myself;

    private final Replicator replicator;
    private final Locks local;
    private final ReplicatedLockStateMachine replicatedLockStateMachine;
    private LogService logService;

    public LeaderOnlyLockManager( MEMBER myself, Replicator replicator, Locks local, ReplicatedLockStateMachine replicatedLockStateMachine, LogService logService )
    {
        this.myself = myself;
        this.replicator = replicator;
        this.local = local;
        this.replicatedLockStateMachine = replicatedLockStateMachine;
        this.logService = logService;
    }

    @Override
    public synchronized Client newClient()
    {
        return new LeaderOnlyLockClient( local.newClient(), replicatedLockStateMachine.currentLockSession(), logService );
    }

    private void requestLock() throws InterruptedException
    {
        // TODO: Don't even try if we are not the leader.

        try
        {
            replicator.replicate( new ReplicatedLockRequest<>( myself, replicatedLockStateMachine.nextId() ));
        }
        catch ( Replicator.ReplicationFailedException e )
        {
            throw new LockClientAlreadyClosedException( "Could not acquire lock session. Leader switch?" );
        }

        synchronized ( replicatedLockStateMachine )
        {
            replicatedLockStateMachine.wait( LOCK_WAIT_TIME );
        }
    }

    @Override
    public void accept( Visitor visitor )
    {
        local.accept( visitor );
    }

    @Override
    public void close()
    {
        local.close();
    }

    private class LeaderOnlyLockClient implements Client
    {
        private final Client localLocks;
        private LockSession lockSession;
        boolean sessionStarted = false;
        private final Log log;

        public LeaderOnlyLockClient( Client localLocks, LockSession lockSession, LogService logService )
        {
            this.localLocks = localLocks;
            this.lockSession = lockSession;
            log = logService.getInternalLog( getClass() );
        }

        private void ensureHoldingReplicatedLock()
        {
            if ( !sessionStarted )
            {
                if ( !lockSession.isMine() )
                {
                    try
                    {
                        requestLock();
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( "Interrupted " );
                    }
                }

                lockSession = replicatedLockStateMachine.currentLockSession();

                if( !lockSession.isMine() )
                {
                    throw new RuntimeException( "Did not manage to acquire valid lock session ID. " + lockSession );
                }

                sessionStarted = true;
            }

            if( !replicatedLockStateMachine.currentLockSession().isMine() )
            {
                throw new RuntimeException( "Local instance lost lock session." );
            }
        }

        @Override
        public void acquireShared( ResourceType resourceType, long resourceId ) throws AcquireLockTimeoutException
        {
            localLocks.acquireShared( resourceType, resourceId );
        }

        @Override
        public void acquireExclusive( ResourceType resourceType, long resourceId ) throws AcquireLockTimeoutException
        {
            log.error( format( "Thread [%d] acquiring %s [%s]", currentThread().getId(), resourceType, resourceId ) );
            ensureHoldingReplicatedLock();
            localLocks.acquireExclusive( resourceType, resourceId );
        }

        @Override
        public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
        {
            log.error( format( "Thread [%d] acquiring %s [%s]", currentThread().getId(), resourceType, resourceId ) );
            ensureHoldingReplicatedLock();
            boolean lockAcquired = localLocks.tryExclusiveLock( resourceType, resourceId );
            log.error( format( "Thread [%d] acquired %s [%s]", currentThread().getId(), resourceType, resourceId ) );
            return lockAcquired;
        }

        @Override
        public boolean trySharedLock( ResourceType resourceType, long resourceId )
        {
            return localLocks.trySharedLock( resourceType, resourceId );
        }

        @Override
        public void releaseShared( ResourceType resourceType, long resourceId )
        {
            localLocks.releaseShared( resourceType, resourceId );
        }

        @Override
        public void releaseExclusive( ResourceType resourceType, long resourceId )
        {
            log.error( format( "Thread [%d] releasing exclusive lock for %s [%s]", currentThread().getId(), resourceType, resourceId ) );
            localLocks.releaseExclusive( resourceType, resourceId );
            log.error( format( "Thread [%d] released exclusive lock for %s [%s]", currentThread().getId(), resourceType, resourceId ) );
        }

        @Override
        public void releaseAll()
        {
            log.error( format( "Thread [%d] release all", currentThread().getId()) );
            localLocks.releaseAll();
        }

        @Override
        public void stop()
        {

            log.error( format( "Thread [%d] stop", currentThread().getId()) );
            localLocks.stop();
        }

        @Override
        public void close()
        {

            log.error( format( "Thread [%d] close", currentThread().getId()) );
            localLocks.close();
        }

        @Override
        public int getLockSessionId()
        {
            return lockSession.id();
        }
    }
}
