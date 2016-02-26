package org.neo4j.coreedge.scenarios;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.coreedge.raft.state.DurableStateStorageImporter;
import org.neo4j.coreedge.raft.state.id_allocation.IdAllocationState;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.KernelEventHandlers;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.kernel.impl.store.id.IdType.ARRAY_BLOCK;
import static org.neo4j.kernel.impl.store.id.IdType.LABEL_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.LABEL_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.NEOSTORE_BLOCK;
import static org.neo4j.kernel.impl.store.id.IdType.NODE;
import static org.neo4j.kernel.impl.store.id.IdType.NODE_LABELS;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY_KEY_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY_KEY_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_GROUP;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_TYPE_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_TYPE_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.SCHEMA;
import static org.neo4j.kernel.impl.store.id.IdType.STRING_BLOCK;

public class ImportClassicStoreCommand
{
    private final File from;
    private final File to;

    public ImportClassicStoreCommand( File from, File to )
    {
        this.from = from;
        this.to = to;
    }

    public void execute() throws Exception
    {
        FileUtils.deleteRecursively(to  );
        copyStore( from, to, filesToExclude( from ) );
        addIdAllocationState(to);
    }

    private void copyStore( File from, File to, Collection<String> excludedFiles ) throws IOException
    {
        FileUtils.copyRecursively( from, to, pathname -> !excludedFiles.contains( pathname.getName() ) );
    }

    private Collection<String> filesToExclude( File classicNeo4jStore )
    {
        Collection<String> filesToExclude = allTheTransactionLogsIn( classicNeo4jStore );
        Collections.addAll( filesToExclude, "neostore", "neostore.id" );
        return filesToExclude;
    }

    private Set<String> allTheTransactionLogsIn( File dbDir )
    {
        Set<String> results = new HashSet<>();
        for ( String s : dbDir.list() )
        {
            if ( s.startsWith( "neostore.transaction.db" ) )
            {
                results.add( s );
            }
        }
        return results;
    }

    private void addIdAllocationState( File coreDir ) throws IOException
    {
        File clusterStateDirectory = new File( coreDir, "cluster-state" );

        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        DefaultIdGeneratorFactory factory = new DefaultIdGeneratorFactory( fileSystem );

        int[] lastIdRangeLengthForMe = new int[]{
                NODE.getGrabSize(),
                RELATIONSHIP.getGrabSize(),
                PROPERTY.getGrabSize(),
                STRING_BLOCK.getGrabSize(),
                ARRAY_BLOCK.getGrabSize(),
                PROPERTY_KEY_TOKEN.getGrabSize(),
                PROPERTY_KEY_TOKEN_NAME.getGrabSize(),
                RELATIONSHIP_TYPE_TOKEN.getGrabSize(),
                RELATIONSHIP_TYPE_TOKEN_NAME.getGrabSize(),
                LABEL_TOKEN.getGrabSize(),
                LABEL_TOKEN_NAME.getGrabSize(),
                NEOSTORE_BLOCK.getGrabSize(),
                SCHEMA.getGrabSize(),
                NODE_LABELS.getGrabSize(),
                RELATIONSHIP_GROUP.getGrabSize()};

        long[] highIds = new long[]{
                getHighId( coreDir, factory, NODE, StoreFactory.NODE_STORE_NAME ),
                getHighId( coreDir, factory, RELATIONSHIP, StoreFactory.RELATIONSHIP_STORE_NAME ),
                getHighId( coreDir, factory, PROPERTY, StoreFactory.PROPERTY_STORE_NAME ),
                getHighId( coreDir, factory, STRING_BLOCK, StoreFactory.PROPERTY_STRINGS_STORE_NAME ),
                getHighId( coreDir, factory, ARRAY_BLOCK, StoreFactory.PROPERTY_ARRAYS_STORE_NAME ),
                getHighId( coreDir, factory, PROPERTY_KEY_TOKEN, StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME ),
                getHighId( coreDir, factory, PROPERTY_KEY_TOKEN_NAME, StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME ),
                getHighId( coreDir, factory, RELATIONSHIP_TYPE_TOKEN, StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME ),
                getHighId( coreDir, factory, RELATIONSHIP_TYPE_TOKEN_NAME, StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME ),
                getHighId( coreDir, factory, LABEL_TOKEN, StoreFactory.LABEL_TOKEN_STORE_NAME ),
                getHighId( coreDir, factory, LABEL_TOKEN_NAME, StoreFactory.LABEL_TOKEN_NAMES_STORE_NAME ),
//                getHighId( coreDir, factory, NEOSTORE_BLOCK, MetaDataStore.DEFAULT_NAME ),
                -1,
                getHighId( coreDir, factory, SCHEMA, StoreFactory.SCHEMA_STORE_NAME ),
                getHighId( coreDir, factory, NODE_LABELS, StoreFactory.NODE_LABELS_STORE_NAME ),
                getHighId( coreDir, factory, RELATIONSHIP_GROUP, StoreFactory.RELATIONSHIP_GROUP_STORE_NAME )};

        IdAllocationState state = new IdAllocationState( highIds, highIds, lastIdRangeLengthForMe, -1 );

        DurableStateStorageImporter<IdAllocationState> storage = new DurableStateStorageImporter<>(
                fileSystem, new File( clusterStateDirectory, "id-allocation-state" ), "id-allocation",
                new IdAllocationState.Marshal(),
                1000, () -> new DatabaseHealth(
                new DatabasePanicEventGenerator( new KernelEventHandlers( NullLog.getInstance() ) ),
                NullLog.getInstance() ), NullLogProvider.getInstance() );

        storage.persist( state );
    }

    private long getHighId( File coreDir, DefaultIdGeneratorFactory factory, IdType idType, String store )
    {
        return factory.open( new File( coreDir, idFile( store ) ), idType.getGrabSize(), idType, -1 ).getHighId();
    }

    private String idFile( String store )
    {
        return MetaDataStore.DEFAULT_NAME + store + ".id";
    }
}
