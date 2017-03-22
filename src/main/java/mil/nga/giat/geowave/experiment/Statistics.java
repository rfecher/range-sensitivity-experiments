package mil.nga.giat.geowave.experiment;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class Statistics
{
	enum DATABASE {
		ACCUMULO,
		HBASE,
		CASSANDRA,
		DYNAMODB,
		NONE
	}

	double[] data;
	int size;
	private final long rangeCount;
	private final long entryCount;
	private static PrintWriter writer = null;

	private static final SimpleDateFormat sdf = new SimpleDateFormat(
			"yyyy-MM-dd_HH-mm-ss");

	public Statistics(
			final double[] data,
			final long rangeCount,
			final long entryCount ) {
		this.data = data;
		this.rangeCount = rangeCount;
		this.entryCount = entryCount;
		size = data.length;
	}

	public static void initializeFile(
			DATABASE database ) {

		try {
			writer = new PrintWriter(
					database.name() + "_statistics-" + sdf.format(new Date()) + ".csv",
					"UTF-8");
			writer.println(getCSVHeader());
		}
		catch (IOException e) {
			System.out.println("Can't write into file. Printing to screen instead");
			writer = null;
		}
	}

	double getMean() {
		double sum = 0.0;
		for (final double a : data) {
			sum += a;
		}
		return sum / size;
	}

	double getVariance() {
		final double mean = getMean();
		double temp = 0;
		for (final double a : data) {
			temp += (a - mean) * (a - mean);
		}
		return temp / size;
	}

	double getStdDev() {
		return Math.sqrt(getVariance());
	}

	public double median() {
		Arrays.sort(data);

		if ((data.length % 2) == 0) {
			return (data[(data.length / 2) - 1] + data[data.length / 2]) / 2.0;
		}
		return data[data.length / 2];
	}

	@Override
	public String toString() {
		return "Statistics [data=" + Arrays.toString(data) + ", size=" + size + ", rangeCount=" + rangeCount
				+ ", entryCount=" + entryCount + "]\n" + toCSVRow();
	}

	public static String getCSVHeader() {
		return "Range Count, Result Count, Mean (ms), Median (ms), Std Dev (ms)";
	}

	public String toCSVRow() {
		return String.format(
				"%s,%s,%s,%s,%s",
				rangeCount,
				entryCount,
				getMean(),
				median(),
				getStdDev());
	}

	public static void printStats(
			final Statistics stats ) {
		// TODO write it to a file instead
		if (stats != null) {
			stats.printStats();
		}

	}

	public void printStats() {
		if (writer == null) {
			System.err.println(toCSVRow());
		}
		else {
			writer.println(toCSVRow());
			System.err.println(toCSVRow());
		}
	}

	public static void closeCSVFile() {
		if (writer != null) writer.close();
	}
}
