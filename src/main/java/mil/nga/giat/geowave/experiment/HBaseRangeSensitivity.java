package mil.nga.giat.geowave.experiment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.index.lexicoder.LongLexicoder;
import mil.nga.giat.geowave.datastore.hbase.util.ConnectionPool;
import mil.nga.giat.geowave.experiment.Statistics.DATABASE;
import mil.nga.giat.geowave.test.HBaseStoreTestEnvironment;
import mil.nga.giat.geowave.test.ZookeeperTestEnvironment;

public class HBaseRangeSensitivity
{

	public static void main(
			final String[] args )
			throws Exception {
		boolean mini = args.length == 0;
		String zookeeper;
		ZookeeperTestEnvironment z = null;
		HBaseStoreTestEnvironment env = null;
		if (mini) {
			z = ZookeeperTestEnvironment.getInstance();
			z.setup();
			System.out.println("Zookeeper url is " + z.getZookeeper());

			env = HBaseStoreTestEnvironment.getInstance();
			env.setup();
			zookeeper = z.getZookeeper();
		}
		else {
			zookeeper = args[0];
		}

		final Connection connection = ConnectionPool.getInstance().getConnection(
				zookeeper);

		Statistics.initializeFile(DATABASE.HBASE);

		final HTableDescriptor table_desc = new HTableDescriptor(
				args[1]);
		// table_desc.addFamily(
		// new HColumnDescriptor(
		// "parition_key"));

		final byte[] cf = StringUtils.stringToBinary("f");
		table_desc.addFamily(new HColumnDescriptor(
				cf));

		if (!connection.getAdmin().tableExists(
				table_desc.getTableName())) {
			final StopWatch sw = new StopWatch();
			connection.getAdmin().createTable(
					table_desc);

			final BufferedMutator writer = connection.getBufferedMutator(table_desc.getTableName());
			final LongLexicoder lexicoder = Lexicoders.LONG;
			long ctr = 0;

			System.out.println("Starting ingestion for HBase");
			sw.start();
			while (ctr < (ExperimentMain.TOTAL * 2)) {
				final RowMutations rowMutation = new RowMutations(
						lexicoder.toByteArray(ctr));

				final byte[] value = new byte[500];
				new Random().nextBytes(value);

				final byte[] row = lexicoder.toByteArray(ctr);
				final Put p = new Put(
						row);
				p.addColumn(
						cf,
						null,
						value);
				rowMutation.add(p);

				writer.mutate(rowMutation.getMutations());
				ctr += 2;
			}
			sw.stop();
			writer.close();
			System.err.println("ingest: " + sw.getTime());
		}
		final Table table = connection.getTable(table_desc.getTableName());

		// TODO write a CSV to file
		System.err.println(Statistics.getCSVHeader());

		Statistics.printStats(allData(
				table,
				1));
		Statistics.printStats(allData(
				table,
				2));
		for (long i = 10; i < ExperimentMain.TOTAL; i *= 10) {
			Statistics.printStats(allData(
					table,
					i));
		}
		Statistics.printStats(allData(
				table,
				ExperimentMain.TOTAL / 2));
		Statistics.printStats(allData(
				table,
				ExperimentMain.TOTAL));
		Statistics.printStats(oneRange(
				table,
				1));
		Statistics.printStats(oneRange(
				table,
				2));
		for (long i = 10; i < ExperimentMain.TOTAL; i *= 10) {
			Statistics.printStats(oneRange(
					table,
					i));
		}
		Statistics.printStats(oneRange(
				table,
				ExperimentMain.TOTAL / 2));
		Statistics.printStats(skipIntervals(
				table,
				1,
				2));
		Statistics.printStats(skipIntervals(
				table,
				2,
				4));
		for (long i = 10; (i * 10) < ExperimentMain.TOTAL; i *= 10) {
			Statistics.printStats(skipIntervals(
					table,
					i,
					i * 10));
		}
		Statistics.closeCSVFile();
		if (env != null) {
			env.tearDown();
		}
		if (z != null) {
			z.tearDown();
		}
	}

	private static Statistics allData(
			final Table t,
			final long interval )
			throws IOException {

		final LongLexicoder lexicoder = Lexicoders.LONG;
		final double[] scanResults = new double[ExperimentMain.SAMPLE_SIZE];
		long rangeCnt = 0;
		final long expectedResults = ExperimentMain.TOTAL;
		if ((ExperimentMain.TOTAL / interval) > ExperimentMain.MAX_RANGES) {
			return null;
		}

		for (int i = 0; i < ExperimentMain.SAMPLE_SIZE; i++) {
			final StopWatch sw = new StopWatch();
			long ctr = 0;
			final Scan scan = new Scan(
					lexicoder.toByteArray(0L),
					lexicoder.toByteArray(ExperimentMain.TOTAL * 2));
			final List<RowRange> rowRanges = new ArrayList<>();
			for (long j = 0; j < (ExperimentMain.TOTAL * 2); j += (interval * 2)) {
				final RowRange rowRange = new RowRange(
						lexicoder.toByteArray(j),
						true,
						lexicoder.toByteArray((j + (interval * 2)) - 1),
						false);
				rowRanges.add(rowRange);
				++rangeCnt;
			}
			scan.setFilter(new MultiRowRangeFilter(
					rowRanges));
			final ResultScanner scanResult = t.getScanner(scan);
			final Iterator<Result> rIt = scanResult.iterator();
			sw.start();
			while (rIt.hasNext()) {
				rIt.next();
				ctr++;
			}
			sw.stop();
			if (ctr != ExperimentMain.TOTAL) {
				System.err.println("experimentFullScan " + interval + " " + ctr);
			}
			scanResults[i] = sw.getTime();
			scanResult.close();
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	}

	private static Statistics oneRange(
			final Table t,
			final long cnt )
			throws IOException {
		final LongLexicoder lexicoder = Lexicoders.LONG;
		final double[] scanResults = new double[ExperimentMain.SAMPLE_SIZE];
		long rangeCnt = 0;
		final long expectedResults = cnt;
		for (int i = 0; i < ExperimentMain.SAMPLE_SIZE; i++) {
			final StopWatch sw = new StopWatch();
			final long start = ((ExperimentMain.TOTAL * 2) - (cnt * 2)) / 2L;
			final Scan scan = new Scan(
					lexicoder.toByteArray(start),
					lexicoder.toByteArray(start + (cnt * 2)));

			final ResultScanner scanResult = t.getScanner(scan);
			rangeCnt = 1;
			final Iterator<Result> it = scanResult.iterator();
			long ctr = 0;
			sw.start();
			while (it.hasNext()) {
				it.next();
				ctr++;
			}
			sw.stop();

			if (ctr != cnt) {
				System.err.println("extraData " + cnt + " " + ctr);
			}
			scanResults[i] = sw.getTime();
			scanResult.close();
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	}

	private static Statistics skipIntervals(
			final Table t,
			final long interval,
			final long skipCnt )
			throws TableNotFoundException,
			IOException {
		final LongLexicoder lexicoder = Lexicoders.LONG;
		final double[] scanResults = new double[ExperimentMain.SAMPLE_SIZE];
		long rangeCnt = 0;
		final long expectedResults = (long) Math.ceil((double) ExperimentMain.TOTAL / (double) skipCnt) * interval;
		for (int i = 0; i < ExperimentMain.SAMPLE_SIZE; i++) {
			final StopWatch sw = new StopWatch();
			final List<RowRange> rowRanges = new ArrayList<>();
			for (long j = 0; j < (ExperimentMain.TOTAL * 2); j += (skipCnt * 2)) {
				rowRanges.add(new RowRange(
						lexicoder.toByteArray(j),
						true,
						lexicoder.toByteArray(j + (interval * 2)),
						false));
			}
			if (rowRanges.size() > ExperimentMain.MAX_RANGES) {
				return null;
			}
			final Scan scan = new Scan(
					lexicoder.toByteArray(0L),
					lexicoder.toByteArray(ExperimentMain.TOTAL * 2));
			scan.setFilter(new MultiRowRangeFilter(
					rowRanges));
			final ResultScanner scanResult = t.getScanner(scan);
			final Iterator<Result> it = scanResult.iterator();
			rangeCnt = rowRanges.size();
			long ctr = 0;
			sw.start();
			while (it.hasNext()) {
				it.next();
				ctr++;
			}
			sw.stop();

			if (ctr != expectedResults) {
				System.err.println("experimentSkipScan " + interval + " " + ctr);
			}
			scanResults[i] = sw.getTime();
			scanResult.close();
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	}
}
