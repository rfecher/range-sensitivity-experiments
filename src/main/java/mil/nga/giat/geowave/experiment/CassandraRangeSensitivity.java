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
import mil.nga.giat.geowave.experiment.Statistics.DATABASE;
import mil.nga.giat.geowave.test.CassandraStoreTestEnvironment;

public class CassandraRangeSensitivity
{
	private static String keyspaceName = "test_keyspace";
	private static String tableName = "test";
	private static String partitionKeyName = "partition_key";
	private static String clusterKeyName = "cluster_key";
	private static String dataColName = "data";
	private static PreparedStatement insertQuery = null;
	private static PreparedStatement rangeQuery = null;
	private static long partitionVal = 12345;

	public static void main(
			final String[] args )
			throws Exception {

		CassandraStoreTestEnvironment env = null;
		final CassandraRequiredOptions options = new CassandraRequiredOptions();
		options.setGeowaveNamespace(keyspaceName);
		if (args.length == 0) {
			env = CassandraStoreTestEnvironment.getInstance();
			options.setContactPoint("127.0.0.1");
			env.setup();
		}
		else {
			options.setContactPoint(args[0]);
			tableName = args[1];
		}

		final CassandraOperations operations = new CassandraOperations(
				options);
		operations.getSession().execute(
				"Use " + keyspaceName);
		if (!operations.tableExists(tableName)) {
			/**
			 * Create the table
			 */
			final Create create = operations.getCreateTable(tableName);
			create.addPartitionKey(
					partitionKeyName,
					DataType.bigint());
			create.addClusteringColumn(
					clusterKeyName,
					DataType.bigint());
			create.addColumn(
					dataColName,
					DataType.blob());
			operations.executeCreateTable(
					create,
					tableName);

			/**
			 * Insert helper
			 */

			insertQuery = operations.getSession().prepare(
					"insert into " + tableName + " (partition_key, cluster_key, data) values (12345, ?, ?)");
			long ctr = args.length > 2 ? Integer.parseInt(args[2]) : 0;
			StopWatch sw = new StopWatch();
			sw.start();
			long total = (args.length > 3 ? Integer.parseInt(args[3]) : ExperimentMain.TOTAL);
			while (ctr < total * 2) {

				final byte[] value = new byte[500];
				new Random().nextBytes(value);

				BoundStatement statement = insertQuery.bind(
						ctr,
						ByteBuffer.wrap(value));
				operations.getSession().execute(
						statement);
				// insertHelper.value(partitionKeyName, partitionVal)
				// .value(clusterKeyName, ctr)
				// .value(dataColName, value);

				ctr += 2;
			}
			sw.stop();
			/**
			 * BoundStatement example = rangeQuery.bind(2L,4L); ResultSet
			 * results = operations.getSession().execute(example);
			 * 
			 * Row row; while( (row = results.one()) != null){
			 * System.out.println( "CAME HERE " + row.getLong(1)); return;
			 * 
			 * }
			 **/

			System.err.println("ingest: " + sw.getTime());
		}
		else if (args.length > 2) {
			insertQuery = operations.getSession().prepare(
					"insert into " + tableName + " (partition_key, cluster_key, data) values (12345, ?, ?)");
			long ctr = args.length > 2 ? Integer.parseInt(args[2]) : 0;
			StopWatch sw = new StopWatch();
			sw.start();
			long total = (args.length > 3 ? Integer.parseInt(args[3]) : ExperimentMain.TOTAL);
			while (ctr < total * 2) {

				final byte[] value = new byte[500];
				new Random().nextBytes(value);

				BoundStatement statement = insertQuery.bind(
						ctr,
						ByteBuffer.wrap(value));
				operations.getSession().execute(
						statement);
				// insertHelper.value(partitionKeyName, partitionVal)
				// .value(clusterKeyName, ctr)
				// .value(dataColName, value);

				ctr += 2;
			}
			sw.stop();
			/**
			 * BoundStatement example = rangeQuery.bind(2L,4L); ResultSet
			 * results = operations.getSession().execute(example);
			 * 
			 * Row row; while( (row = results.one()) != null){
			 * System.out.println( "CAME HERE " + row.getLong(1)); return;
			 * 
			 * }
			 **/

			System.err.println("ingest: " + sw.getTime());
		}
		if (args.length < 3) {
			Statistics.initializeFile(DATABASE.CASSANDRA);
			String select = "select " + dataColName + " from " + tableName + " where " + partitionKeyName + " = "
					+ partitionVal + " and " + clusterKeyName + " >= ? and " + clusterKeyName + " < ?";

			System.out.println(select);
			rangeQuery = operations.getSession().prepare(
					select);

			// TODO write a CSV to file
			System.err.println(Statistics.getCSVHeader());

			Statistics.printStats(allData(
					operations,
					1));
			Statistics.printStats(allData(
					operations,
					2));
			for (long i = 10; i < ExperimentMain.TOTAL; i *= 10) {
				Statistics.printStats(allData(
						operations,
						i));
			}
			Statistics.printStats(allData(
					operations,
					ExperimentMain.TOTAL / 2));
			Statistics.printStats(allData(
					operations,
					ExperimentMain.TOTAL));

			Statistics.printStats(oneRange(
					operations,
					1));
			Statistics.printStats(oneRange(
					operations,
					2));
			for (long i = 10; i < ExperimentMain.TOTAL; i *= 10) {
				Statistics.printStats(oneRange(
						operations,
						i));
			}
			Statistics.printStats(oneRange(
					operations,
					ExperimentMain.TOTAL / 2));
			Statistics.printStats(skipIntervals(
					operations,
					1,
					2));
			Statistics.printStats(skipIntervals(
					operations,
					2,
					4));
			for (long i = 10; (i * 10) < ExperimentMain.TOTAL; i *= 10) {
				Statistics.printStats(skipIntervals(
						operations,
						i,
						i * 10));
			}

			Statistics.closeCSVFile();
			if (env != null) {
				env.tearDown();
			}
		}
	}

	private static Statistics allData(
			final CassandraOperations operations,
			final long interval ) {
		double[] scanResults = new double[ExperimentMain.SAMPLE_SIZE];
		long rangeCnt = 0;
		long expectedResults = ExperimentMain.TOTAL;
		if (ExperimentMain.TOTAL / interval > ExperimentMain.MAX_RANGES) {
			return null;
		}
		for (int i = 0; i < ExperimentMain.SAMPLE_SIZE; i++) {
			rangeCnt = 0;
			final StopWatch sw = new StopWatch();

			long ctr = 0;
			sw.start();
			for (long j = 0; j < ExperimentMain.TOTAL * 2; j += (interval * 2)) {
				BoundStatement bound = rangeQuery.bind(
						j,
						(j + interval * 2 - 1));
				ResultSet results = operations.getSession().execute(
						bound);

				rangeCnt += 1;
				while (results.one() != null)
					++ctr;
			}
			sw.stop();

			if (ctr != ExperimentMain.TOTAL) {
				System.err.println("experimentFullScan " + interval + " " + ctr);
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
		double[] scanResults = new double[ExperimentMain.SAMPLE_SIZE];
		long rangeCnt = 0;
		long expectedResults = (long) Math.ceil((double) ExperimentMain.TOTAL / (double) skipCnt) * interval;
		for (int i = 0; i < ExperimentMain.SAMPLE_SIZE; i++) {
			rangeCnt = 0;
			final StopWatch sw = new StopWatch();

			long ctr = 0;
			sw.start();
			for (long j = 0; j < ExperimentMain.TOTAL * 2; j += (skipCnt * 2)) {
				BoundStatement bound = rangeQuery.bind(
						j,
						(j + interval * 2));
				ResultSet results = operations.getSession().execute(
						bound);
				rangeCnt += 1;
				while (results.one() != null)
					++ctr;
			}

			sw.stop();

			if (ctr != expectedResults) {
				System.err.println("experimentSkipScan " + interval + " " + ctr);
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
		double[] scanResults = new double[ExperimentMain.SAMPLE_SIZE];
		long rangeCnt = 0;
		long expectedResults = cnt;
		for (int i = 0; i < ExperimentMain.SAMPLE_SIZE; i++) {
			rangeCnt = 0;
			final StopWatch sw = new StopWatch();

			long ctr = 0;
			long start = (ExperimentMain.TOTAL * 2 - cnt * 2) / 2L;

			sw.start();
			BoundStatement bound = rangeQuery.bind(
					start,
					(start + cnt * 2));
			ResultSet results = operations.getSession().execute(
					bound);
			rangeCnt += 1;
			while (results.one() != null)
				++ctr;

			sw.stop();

			if (ctr != cnt) {
				System.err.println("extraData " + cnt + " " + ctr);
			}
			scanResults[i] = sw.getTime();
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	}
}
