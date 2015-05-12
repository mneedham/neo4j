package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;

public class CoreEdgeTransactionCommitProcess implements TransactionCommitProcess
{

    @Override
    public long commit( TransactionRepresentation representation, LockGroup locks, CommitEvent commitEvent )
            throws TransactionFailureException
    {
        return 0;
    }
}
