package mil.nga.giat.geowave.experiment;

import org.apache.commons.lang3.ArrayUtils;

public class ExperimentMain
{
	public static long TOTAL = 1000000L;
	public static int SAMPLE_SIZE = 10;

	// there's probably a cap on the ranges before it just takes ridiculously
	// too long (and logically we would never exceed), not sure what it is,
	// probably differs per datastore
	// this is mostly here for sanity purposes so we aren't running queries that
	// may never finish and may be not worth benchmarking because they're just
	// too ridiculous in reality (typically at the finest grained query
	// decomposition we end up with 10's of thousands of ranges, now that could
	// be multiplied by the number of hashes/partitions, but there still is some
	// logical cap on the number of ranges that we'll ever realistically use)
	public static long MAX_RANGES = 1000000L;

	public static void main(
			final String[] args )
			throws Exception {
		TOTAL = Long.parseLong(args[1]);
		MAX_RANGES = Long.parseLong(args[2]);
		SAMPLE_SIZE = Integer.parseInt(args[3]);
		switch (args[0]) {
			case "hbase":
				HBaseRangeSensitivity.main(ArrayUtils.subarray(
						args,
						4,
						args.length));
				break;
			case "accumulo":
				AccumuloRangeSensitivity.main(ArrayUtils.subarray(
						args,
						4,
						args.length));
				break;
			case "dynamodb":
				DynamoDBRangeSensitivity.main(ArrayUtils.subarray(
						args,
						4,
						args.length));
				break;

			case "cassandra":
				CassandraRangeSensitivity.main(ArrayUtils.subarray(
						args,
						4,
						args.length));
				break;
		}
	}
}
