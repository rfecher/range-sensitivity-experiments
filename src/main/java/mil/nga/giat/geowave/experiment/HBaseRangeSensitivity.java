package mil.nga.giat.geowave.experiment;

import java.io.IOException;
import java.util.Random;

import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.Text;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.index.lexicoder.LongLexicoder;
import mil.nga.giat.geowave.datastore.hbase.util.ConnectionPool;
import mil.nga.giat.geowave.test.HBaseStoreTestEnvironment;
import mil.nga.giat.geowave.test.ZookeeperTestEnvironment;

public class HBaseRangeSensitivity
{
	private static long TOTAL = 100L;
	private static int SAMPLE_SIZE = 10;

	// there's probably a cap on the ranges before it just takes ridiculously
	// too long (and logically we would never exceed), not sure what it is,
	// probably differs per datastore
	// this is mostly here for sanity purposes so we aren't running queries that
	// may never finish and may be not worth benchmarking because they're just
	// too ridiculous in reality (typically at the finest grained query
	// decomposition we end up with 10's of thousands of ranges, now that could
	// be multiplied by the number of hashes/partitions, but there still is some
	// logical cap on the number of ranges that we'll ever realistically use)
	private static long MAX_RANGES = 1000L;

	public static void main(
			final String[] args )
			throws Exception {

		final ZookeeperTestEnvironment z = ZookeeperTestEnvironment.getInstance();
		z.setup();
		System.out.println(
				"Zookeeper url is " + z.getZookeeper());

		final HBaseStoreTestEnvironment env = HBaseStoreTestEnvironment.getInstance();
		env.setup();

		Connection connection = ConnectionPool.getInstance().getConnection(
				z.getZookeeper());

		NamespaceDescriptor namespace_desc = NamespaceDescriptor.create(
				"simple_test2").build();
		connection.getAdmin().createNamespace(
				namespace_desc);

		HTableDescriptor table_desc = new HTableDescriptor(
				"test2");
//		table_desc.addFamily(
//				new HColumnDescriptor(
//						"parition_key"));
		byte[] cf = StringUtils.stringToBinary("f");
		table_desc.addFamily(
				new HColumnDescriptor(
						cf));

		connection.getAdmin().createTable(
				table_desc);

		BufferedMutator writer = connection.getBufferedMutator(
				table_desc.getTableName());
		final LongLexicoder lexicoder = Lexicoders.LONG;
		long ctr = 0;
		StopWatch sw = new StopWatch();
		
		System.out.println("Starting ingestion for HBase");
		sw.start();
		while (ctr < TOTAL * 2) {
			final RowMutations rowMutation = new RowMutations(
					lexicoder.toByteArray(
							ctr));

			final byte[] value = new byte[500];
			new Random().nextBytes(
					value);

			byte[] row = lexicoder.toByteArray(
					ctr);
			Put p = new Put(row);
			p.add(
					new KeyValue(
							row,
							cf,
							null,
							value));
			rowMutation.add(
					p);

			writer.mutate(
					rowMutation.getMutations());
			ctr += 2;
		}
		sw.stop();
		writer.close();

		Table table = connection.getTable(
				table_desc.getTableName());

		System.err.println(
				"ingest: " + sw.getTime());

		// TODO write a CSV to file
		System.err.println(
				Statistics.getCSVHeader());

		Statistics.printStats(
				allData(
						table,
						1));
		Statistics.printStats(
				allData(
						table,
						2));
		for (long i = 10; i < TOTAL; i *= 10) {
			Statistics.printStats(
					allData(
							table,
							i));
		}
		Statistics.printStats(
				allData(
						table,
						TOTAL / 2));
		Statistics.printStats(
				allData(
						table,
						TOTAL));
		/**
		 * Statistics.printStats( oneRange( c, 1)); Statistics.printStats(
		 * oneRange( c, 2)); for (long i = 10; i < TOTAL; i *= 10) {
		 * Statistics.printStats( oneRange( c, i)); } Statistics.printStats(
		 * oneRange( c, TOTAL / 2)); Statistics.printStats( skipIntervals( c, 1,
		 * 2)); Statistics.printStats( skipIntervals( c, 2, 4)); for (long i =
		 * 10; (i * 10) < TOTAL; i *= 10) { Statistics.printStats(
		 * skipIntervals( c, i, i * 10)); }
		 **/
		z.tearDown();
		env.tearDown();
		System.exit(0);
	}

	private static Statistics allData(
			final Table t,
			final long interval )
			throws IOException {

		final LongLexicoder lexicoder = Lexicoders.LONG;
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

				Scan scan = new Scan(
						lexicoder.toByteArray(
								j),
						lexicoder.toByteArray(
								j + interval * 2 - 1));

				ResultScanner scanResult = t.getScanner(
						scan);

				for (Result rr = scanResult.next(); rr != null; rr = scanResult.next()) {
					++ctr;
				}
				++rangeCnt;
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
	/**
	 * private static Statistics skipIntervals( final Connector c, final long
	 * interval, final long skipCnt ) throws TableNotFoundException { final
	 * LongLexicoder lexicoder = Lexicoders.LONG; double[] scanResults = new
	 * double[SAMPLE_SIZE]; long rangeCnt = 0; long expectedResults = (long)
	 * Math.ceil( (double) TOTAL / (double) skipCnt) * interval; for (int i = 0;
	 * i < SAMPLE_SIZE; i++) { final StopWatch sw = new StopWatch(); final
	 * BatchScanner s = c.createBatchScanner( "test", new Authorizations(), 16);
	 * List<Range> ranges = new ArrayList<Range>(); for (long j = 0; j < TOTAL *
	 * 2; j += (skipCnt * 2)) { ranges.add( new Range( new Text(
	 * lexicoder.toByteArray( j)), true,
	 * 
	 * new Text( lexicoder.toByteArray( j + (interval * 2))), false)); } if
	 * (ranges.size() > MAX_RANGES) { return null; } s.setRanges( ranges);
	 * rangeCnt = ranges.size(); final Iterator<Entry<Key, Value>> it =
	 * s.iterator(); long ctr = 0; sw.start(); while (it.hasNext()) { it.next();
	 * ctr++; } sw.stop();
	 * 
	 * if (ctr != expectedResults) { System.err.println( "experimentSkipScan " +
	 * interval + " " + ctr); } scanResults[i] = sw.getTime(); s.close(); }
	 * return new Statistics( scanResults, rangeCnt, expectedResults); }
	 * 
	 * private static Statistics oneRange( final Connector c, final long cnt )
	 * throws TableNotFoundException { final LongLexicoder lexicoder =
	 * Lexicoders.LONG; double[] scanResults = new double[SAMPLE_SIZE]; long
	 * rangeCnt = 0; long expectedResults = cnt; for (int i = 0; i <
	 * SAMPLE_SIZE; i++) { final StopWatch sw = new StopWatch(); final
	 * BatchScanner s = c.createBatchScanner( "test", new Authorizations(), 16);
	 * List<Range> ranges = new ArrayList<Range>(); long start = (TOTAL * 2 -
	 * cnt * 2) / 2L; ranges.add( new Range( new Text( lexicoder.toByteArray(
	 * start)), true,
	 * 
	 * new Text( lexicoder.toByteArray( start + cnt * 2)), false)); s.setRanges(
	 * ranges); rangeCnt = ranges.size(); final Iterator<Entry<Key, Value>> it =
	 * s.iterator(); long ctr = 0; sw.start(); while (it.hasNext()) { it.next();
	 * ctr++; } sw.stop();
	 * 
	 * if (ctr != cnt) { System.err.println( "extraData " + cnt + " " + ctr); }
	 * scanResults[i] = sw.getTime(); s.close(); } return new Statistics(
	 * scanResults, rangeCnt, expectedResults); }
	 **/
}
