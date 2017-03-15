package mil.nga.giat.geowave.experiment;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.commons.lang3.time.StopWatch;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.schemabuilder.Create;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraRequiredOptions;
import mil.nga.giat.geowave.test.CassandraStoreTestEnvironment;

public class CassandraRangeSensitivity
{
	private static long TOTAL = 100000L;
	private static int SAMPLE_SIZE = 10;
	private static String keyspaceName = "test_keyspace";
	private static String tableName = "test";
	private static String partitionKeyName = "partition_key";
	private static String clusterKeyName = "cluster_key";
	private static String dataColName = "data";
	private static PreparedStatement insertQuery = null;
	private static PreparedStatement rangeQuery = null;
	private static long partitionVal = 12345;
	
	// there's probably a cap on the ranges before it just takes ridiculously
	// too long (and logically we would never exceed), not sure what it is,
	// probably differs per datastore
	// this is mostly here for sanity purposes so we aren't running queries that
	// may never finish and may be not worth benchmarking because they're just
	// too ridiculous in reality (typically at the finest grained query
	// decomposition we end up with 10's of thousands of ranges, now that could
	// be multiplied by the number of hashes/partitions, but there still is some
	// logical cap on the number of ranges that we'll ever realistically use)
	private static long MAX_RANGES = 1000000L;

	public static void main(
			final String[] args )
			throws Exception {
	
		final CassandraStoreTestEnvironment env = CassandraStoreTestEnvironment.getInstance();
		env.setup();
		
		final CassandraRequiredOptions options = new CassandraRequiredOptions();
		options.setContactPoint("127.0.0.1");
		options.setGeowaveNamespace(keyspaceName);
		final CassandraOperations operations = new CassandraOperations(options);
		
		operations.getSession().execute("Use " + keyspaceName);
		/**
		 * Create the table
		 */
		final Create create = operations.getCreateTable(tableName);		
		create.addPartitionKey(partitionKeyName, DataType.bigint());
		create.addClusteringColumn(clusterKeyName, DataType.bigint());
		create.addColumn(dataColName, DataType.blob());
		operations.executeCreateTable(create, tableName); 
		
		/**
		 * Insert helper
		 */
		
		insertQuery = operations.getSession().prepare(
				  "insert into test (partition_key, cluster_key, data) values (12345, ?, ?)");
		
		long ctr = 0;
		StopWatch sw = new StopWatch();
		sw.start();
		while (ctr < TOTAL * 2) {
			
			final byte[] value = new byte[500];
			new Random().nextBytes(
					value);
			
			BoundStatement statement = insertQuery.bind(ctr, ByteBuffer.wrap(value));
			operations.getSession().execute(statement);
//			insertHelper.value(partitionKeyName, partitionVal)
//			.value(clusterKeyName, ctr)
//			.value(dataColName, value);
			
			ctr += 2;
		}
		sw.stop();
		
		String select = "select " + dataColName + " from " + tableName + " where " + 
				   partitionKeyName + " = " + partitionVal + " and " +  clusterKeyName +
				   " >= ? and " + clusterKeyName + " < ?";
		
		System.out.println(select);
		rangeQuery = operations.getSession().prepare(select);
		
		/**BoundStatement example = rangeQuery.bind(2L,4L);
 		ResultSet results = operations.getSession().execute(example);
		 
		Row row;
		while( (row = results.one()) != null){
			System.out.println("CAME HERE " + row.getLong(1));
			return;
			
		} **/
		
		System.err.println(
				"ingest: " + sw.getTime());

		// TODO write a CSV to file
		System.err.println(
				Statistics.getCSVHeader());

		
		Statistics.printStats(
				allData(
						operations,
						1));
		Statistics.printStats(
				allData(
						operations,
						2));
		for (long i = 10; i < TOTAL; i *= 10) {
			Statistics.printStats(
					allData(
							operations,
							i));
		}
		Statistics.printStats(
				allData(
						operations,
						TOTAL / 2));
		Statistics.printStats(
				allData(
						operations,
						TOTAL));

		Statistics.printStats(
				oneRange(
						operations,
						1));
		Statistics.printStats(
				oneRange(
						operations,
						2));
		for (long i = 10; i < TOTAL; i *= 10) {
			Statistics.printStats(
					oneRange(
							operations,
							i));
		}
		Statistics.printStats(
				oneRange(
						operations,
						TOTAL / 2));
		Statistics.printStats(
				skipIntervals(
						operations,
						1,
						2));
		Statistics.printStats(
				skipIntervals(
						operations,
						2,
						4));
		for (long i = 10; (i * 10) < TOTAL; i *= 10) {
			Statistics.printStats(
					skipIntervals(
							operations,
							i,
							i * 10));
		} 
		
		
		env.tearDown();
		
	}

	private static Statistics allData(
			final CassandraOperations operations,
			final long interval )
	{
		double[] scanResults = new double[SAMPLE_SIZE];
		long rangeCnt = 0;
		long expectedResults = TOTAL;
		if (TOTAL / interval > MAX_RANGES) {
			return null;
		}
		for (int i = 0; i < SAMPLE_SIZE; i++) {
			final StopWatch sw = new StopWatch();
			
			long ctr = 0;
			sw.start();
			for (long j = 0; j < TOTAL * 2; j += (interval * 2)) {
				BoundStatement bound = rangeQuery.bind(j, (j + interval * 2 - 1));
				ResultSet results = operations.getSession().execute(bound);
				
				while(results.one() != null)
					++ctr;
			}
			sw.stop();
			
			if (ctr != TOTAL) {
				System.err.println(
						"experimentFullScan " + interval + " " + ctr);
			}
			scanResults[i] = sw.getTime();
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	}


	private static Statistics skipIntervals(
			final CassandraOperations operations,
			final long interval,
			final long skipCnt ) {
		double[] scanResults = new double[SAMPLE_SIZE];
		long rangeCnt = 0;
		long expectedResults = (long) Math.ceil(
				(double) TOTAL / (double) skipCnt) * interval;
		for (int i = 0; i < SAMPLE_SIZE; i++) {
			final StopWatch sw = new StopWatch();
			
			long ctr = 0;
			sw.start();
			for (long j = 0; j < TOTAL * 2; j += (skipCnt * 2)) {
				BoundStatement bound = rangeQuery.bind(j, (j + interval * 2));
				ResultSet results = operations.getSession().execute(bound);
				
				while(results.one() != null)
					++ctr;
			}
		
			sw.stop();

			if (ctr != expectedResults) {
				System.err.println(
						"experimentSkipScan " + interval + " " + ctr);
			}
			scanResults[i] = sw.getTime();
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	}

	private static Statistics oneRange(
			final CassandraOperations operations,
			final long cnt ) {
		double[] scanResults = new double[SAMPLE_SIZE];
		long rangeCnt = 0;
		long expectedResults = cnt;
		for (int i = 0; i < SAMPLE_SIZE; i++) {
			final StopWatch sw = new StopWatch();

			long ctr = 0;
			long start = (TOTAL * 2 - cnt * 2) / 2L;
			
			sw.start();
			BoundStatement bound = rangeQuery.bind(start, (start + cnt * 2));
			ResultSet results = operations.getSession().execute(bound);
			
			while(results.one() != null)
				++ctr;
			
			sw.stop();
		
			if (ctr != cnt) {
				System.err.println(
						"extraData " + cnt + " " + ctr);
			}
			scanResults[i] = sw.getTime();
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	} 
}
