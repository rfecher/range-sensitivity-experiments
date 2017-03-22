package mil.nga.giat.geowave.experiment;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.io.Text;

import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.index.lexicoder.LongLexicoder;
import mil.nga.giat.geowave.datastore.accumulo.util.ConnectorPool;
import mil.nga.giat.geowave.experiment.Statistics.DATABASE;
import mil.nga.giat.geowave.test.AccumuloStoreTestEnvironment;
import mil.nga.giat.geowave.test.ZookeeperTestEnvironment;

public class AccumuloRangeSensitivity
{

	public static void main(
			final String[] args )
			throws Exception {

		boolean mini = args.length == 0;
		ZookeeperTestEnvironment z = null;
		AccumuloStoreTestEnvironment a = null;
		String zk, in, u, p;
		if (mini) {
			z = ZookeeperTestEnvironment.getInstance();
			z.setup();
			a = AccumuloStoreTestEnvironment.getInstance();
			a.setup();
			zk = a.getZookeeper();
			in = a.getAccumuloInstance();
			u = a.getAccumuloUser();
			p = a.getAccumuloPassword();
		}
		else {
			zk = args[0];
			in = args[1];
			u = args[2];
			p = args[3];
		}
		Statistics.initializeFile(DATABASE.ACCUMULO);

		final Connector c = ConnectorPool.getInstance().getConnector(
				zk,
				in,
				u,
				p);
		if (!c.tableOperations().exists(
				args[4])) {
			c.tableOperations().create(
					args[4]);
			c.tableOperations().setProperty(
					args[4],
					"table.cache.block.enable",
					"false");
			c.tableOperations().setProperty(
					args[4],
					"table.cache.index.enable",
					"false");
			final BatchWriter w = c.createBatchWriter(
					args[4],
					new BatchWriterConfig());

			System.out.println("Starting Ingestion for Accumulo");
			final LongLexicoder lexicoder = Lexicoders.LONG;
			long ctr = 0;
			StopWatch sw = new StopWatch();
			sw.start();
			while (ctr < ExperimentMain.TOTAL * 2) {
				final Mutation m = new Mutation(
						new Text(
								lexicoder.toByteArray(ctr)));
				ctr += 2;
				final byte[] value = new byte[500];
				new Random().nextBytes(value);
				m.put(
						new Text(),
						new Text(),
						new Value(
								value));
				w.addMutation(m);
			}
			sw.stop();
			w.close();
			c.tableOperations().compact(
					args[4],
					null,
					null,
					true,
					true);
			System.err.println("ingest: " + sw.getTime());

		}
		// TODO write a CSV to file
		System.err.println(Statistics.getCSVHeader());

		Statistics.printStats(allData(
				c,
				args[4],
				1));
		Statistics.printStats(allData(
				c,
				args[4],
				2));
		for (long i = 10; i < ExperimentMain.TOTAL; i *= 10) {
			Statistics.printStats(allData(
					c,
					args[4],
					i));
		}
		Statistics.printStats(allData(
				c,
				args[4],
				ExperimentMain.TOTAL / 2));
		Statistics.printStats(allData(
				c,
				args[4],
				ExperimentMain.TOTAL));

		Statistics.printStats(oneRange(
				c,
				args[4],
				1));
		Statistics.printStats(oneRange(
				c,
				args[4],
				2));
		for (long i = 10; i < ExperimentMain.TOTAL; i *= 10) {
			Statistics.printStats(oneRange(
					c,
					args[4],
					i));
		}
		Statistics.printStats(oneRange(
				c,
				args[4],
				ExperimentMain.TOTAL / 2));
		Statistics.printStats(skipIntervals(
				c,
				args[4],
				1,
				2));
		Statistics.printStats(skipIntervals(
				c,
				args[4],
				2,
				4));
		for (long i = 10; (i * 10) < ExperimentMain.TOTAL; i *= 10) {
			Statistics.printStats(skipIntervals(
					c,
					args[4],
					i,
					i * 10));
		}

		Statistics.closeCSVFile();
		if (a != null) {
			a.tearDown();
			z.tearDown();
		}
	}

	private static Statistics allData(
			final Connector c,
			final String tablename,
			final long interval )
			throws TableNotFoundException {
		final LongLexicoder lexicoder = Lexicoders.LONG;
		double[] scanResults = new double[ExperimentMain.SAMPLE_SIZE];
		long rangeCnt = 0;
		long expectedResults = ExperimentMain.TOTAL;
		if (ExperimentMain.TOTAL / interval > ExperimentMain.MAX_RANGES) {
			return null;
		}
		for (int i = 0; i < ExperimentMain.SAMPLE_SIZE; i++) {
			final StopWatch sw = new StopWatch();
			final BatchScanner s = c.createBatchScanner(
					tablename,
					new Authorizations(),
					16);
			List<Range> ranges = new ArrayList<Range>();
			for (long j = 0; j < ExperimentMain.TOTAL * 2; j += (interval * 2)) {
				ranges.add(new Range(
						new Text(
								lexicoder.toByteArray(j)),
						true,

						new Text(
								lexicoder.toByteArray(j + interval * 2 - 1)),
						false));
			}
			s.setRanges(ranges);
			rangeCnt = ranges.size();
			final Iterator<Entry<Key, Value>> it = s.iterator();
			long ctr = 0;
			sw.start();
			while (it.hasNext()) {
				it.next();
				ctr++;
			}
			sw.stop();

			if (ctr != ExperimentMain.TOTAL) {
				System.err.println("experimentFullScan " + interval + " " + ctr);
			}
			scanResults[i] = sw.getTime();
			s.close();
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	}

	private static Statistics skipIntervals(
			final Connector c,
			final String tablename,
			final long interval,
			final long skipCnt )
			throws TableNotFoundException {
		final LongLexicoder lexicoder = Lexicoders.LONG;
		double[] scanResults = new double[ExperimentMain.SAMPLE_SIZE];
		long rangeCnt = 0;
		long expectedResults = (long) Math.ceil((double) ExperimentMain.TOTAL / (double) skipCnt) * interval;
		for (int i = 0; i < ExperimentMain.SAMPLE_SIZE; i++) {
			final StopWatch sw = new StopWatch();
			final BatchScanner s = c.createBatchScanner(
					tablename,
					new Authorizations(),
					16);
			List<Range> ranges = new ArrayList<Range>();
			for (long j = 0; j < ExperimentMain.TOTAL * 2; j += (skipCnt * 2)) {
				ranges.add(new Range(
						new Text(
								lexicoder.toByteArray(j)),
						true,

						new Text(
								lexicoder.toByteArray(j + (interval * 2))),
						false));
			}
			if (ranges.size() > ExperimentMain.MAX_RANGES) {
				return null;
			}
			s.setRanges(ranges);
			rangeCnt = ranges.size();
			final Iterator<Entry<Key, Value>> it = s.iterator();
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
			s.close();
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	}

	private static Statistics oneRange(
			final Connector c,
			final String tablename,
			final long cnt )
			throws TableNotFoundException {
		final LongLexicoder lexicoder = Lexicoders.LONG;
		double[] scanResults = new double[ExperimentMain.SAMPLE_SIZE];
		long rangeCnt = 0;
		long expectedResults = cnt;
		for (int i = 0; i < ExperimentMain.SAMPLE_SIZE; i++) {
			final StopWatch sw = new StopWatch();
			final BatchScanner s = c.createBatchScanner(
					tablename,
					new Authorizations(),
					16);
			List<Range> ranges = new ArrayList<Range>();
			long start = (ExperimentMain.TOTAL * 2 - cnt * 2) / 2L;
			ranges.add(new Range(
					new Text(
							lexicoder.toByteArray(start)),
					true,

					new Text(
							lexicoder.toByteArray(start + cnt * 2)),
					false));
			s.setRanges(ranges);
			rangeCnt = ranges.size();
			final Iterator<Entry<Key, Value>> it = s.iterator();
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
			s.close();
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	}
}
