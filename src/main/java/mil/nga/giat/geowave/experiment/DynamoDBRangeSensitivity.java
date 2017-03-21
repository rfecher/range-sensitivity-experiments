package mil.nga.giat.geowave.experiment;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tomcat.jni.Time;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOptions;
import mil.nga.giat.geowave.experiment.Statistics.DATABASE;
import mil.nga.giat.geowave.test.DynamoDBTestEnvironment;

public class DynamoDBRangeSensitivity
{ 
	private static long TOTAL = 10000L;
	private static int SAMPLE_SIZE = 10;
	private static String tableName = "test";
	private static String partitionKeyName = "partition_key";
	private static String sortKeyName = "sort_key";
	private static String dataColName = "data";
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
	private static long MAX_RANGES = 100000L;

	public static void main(
			final String[] args )
			throws Exception {

		DynamoDBTestEnvironment env = DynamoDBTestEnvironment.getInstance();
		env.setup();
		
		Statistics.initializeFile(DATABASE.DYNAMODB);
		final DynamoDBOptions options = new DynamoDBOptions();
		options.setEndpoint("http://127.0.0.1:8000");
		DynamoDBOperations operations = new DynamoDBOperations(options);
		
		
		/**
		 * Create the table
		 */
		ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement()
                  .withAttributeName(partitionKeyName)
                  .withKeyType(KeyType.HASH)); //Partition key
        
        keySchema.add(new KeySchemaElement()
                .withAttributeName(sortKeyName)
                .withKeyType(KeyType.RANGE)); 

        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition()
                     .withAttributeName(partitionKeyName)
                     .withAttributeType("N"));
        
        attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName(sortKeyName)
                .withAttributeType("N"));
		

        CreateTableRequest createTableRequest = new CreateTableRequest()
        		.withTableName(tableName)
        		.withKeySchema(keySchema)
        		.withAttributeDefinitions(attributeDefinitions)
        		.withProvisionedThroughput(new ProvisionedThroughput()
        				.withReadCapacityUnits(5L)
        				.withWriteCapacityUnits(6L));
		
        
        CreateTableResult tableResult = operations.getClient().createTable(createTableRequest);
        
        while(!tableResult.getTableDescription().getTableStatus().equals("ACTIVE")){
        	System.out.println(" Table state is " + tableResult.getTableDescription().getTableStatus());
        	Time.sleep(1);
        }
        
        
        System.out.println(" Starting ingestion for dynamoDB ");
		long ctr = 0;
		StopWatch sw = new StopWatch();
		sw.start();
		while (ctr < TOTAL * 2) {
			
			final byte[] value = new byte[500];
			new Random().nextBytes(
					value);
			
			Map<String, AttributeValue> items = new HashMap<>();
			items.put(partitionKeyName, new AttributeValue().withN("12345"));
			items.put(sortKeyName, new AttributeValue().withN(Long.toString(ctr)));
			items.put(dataColName, new AttributeValue().withBS(ByteBuffer.wrap(value)));
			
//			Item item = new Item().withPrimaryKey(partitionKeyName, partitionVal).
//									withNumber(sortKeyName, ctr).withBinary(dataColName, value);
			
			operations.getClient().putItem(tableName,  items);
			
			ctr += 2;
			
		}
		sw.stop();
				
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

		Statistics.closeCSVFile();
		env.tearDown();
	}

	private static Statistics allData(
			final DynamoDBOperations operations,
			final long interval ) {
		double[] scanResults = new double[SAMPLE_SIZE];
		long rangeCnt = 0;
		long expectedResults = TOTAL;
		if (TOTAL / interval > MAX_RANGES) {
			return null;
		}
		
		
		for (int i = 0; i < SAMPLE_SIZE; i++) {
			final StopWatch sw = new StopWatch();

			List<QueryRequest> requests = new ArrayList<QueryRequest>();
			for (long j = 0; j < TOTAL * 2; j += (interval * 2)) {
				String condition = new String(partitionKeyName + "= :val AND " +
					    sortKeyName + " BETWEEN :startJ AND :endJ" );
				
				QueryRequest request = new QueryRequest();
				request.setTableName(tableName);
				request.addExpressionAttributeValuesEntry(":val", new AttributeValue().withN("12345"));
				request.addExpressionAttributeValuesEntry(":startJ", new AttributeValue().withN(Long.toString(j)));
				request.addExpressionAttributeValuesEntry(":endJ", new AttributeValue().withN(Long.toString(j + interval * 2 - 1)));
				request.setKeyConditionExpression(condition);
				requests.add(request);
				
			}
			
			long ctr = 0;
			sw.start();
			for(QueryRequest request : requests){
				QueryResult result = operations.getClient().query(request);
			
				//System.out.println(" Count is  " +result.getCount() + " scanned count is " + result.getScannedCount());
				
				Iterator<Map<String, AttributeValue>> firstIter = result.getItems().iterator();
				
				ctr += result.getCount();
				while (firstIter.hasNext()) {
					firstIter.next();
					//ctr++;
				}
				
		
				while((result.getLastEvaluatedKey() != null) && !result.getLastEvaluatedKey().isEmpty()){
					request.setExclusiveStartKey(result.getLastEvaluatedKey());
					result = operations.getClient().query(request);
					
					Iterator<Map<String, AttributeValue>> it = result.getItems().iterator();

					ctr += result.getCount();
					while (it.hasNext()) {
						it.next();
						//ctr++;
					}
					
				}				
			}
			
			sw.stop();
			rangeCnt = requests.size();
			
			if (ctr != TOTAL) {
				System.err.println(
						"ERROR: experimentFullScan " + interval + " " + ctr);
			}
			scanResults[i] = sw.getTime();
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	}

	private static Statistics skipIntervals(
			final DynamoDBOperations operations,
			final long interval,
			final long skipCnt ) {
		double[] scanResults = new double[SAMPLE_SIZE];
		long rangeCnt = 0;
		long expectedResults = (long) Math.ceil(
				(double) TOTAL / (double) skipCnt) * interval;
		
		
		for (int i = 0; i < SAMPLE_SIZE; i++) {
			final StopWatch sw = new StopWatch();

			List<QueryRequest> requests = new ArrayList<QueryRequest>();
			for (long j = 0; j < TOTAL * 2; j += (skipCnt * 2)) {
				String condition = new String(partitionKeyName + "= :val  AND " +
					    sortKeyName + " BETWEEN :startJ AND :endJ" );
				
				QueryRequest request = new QueryRequest();
				request.addExpressionAttributeValuesEntry(":val", new AttributeValue().withN(Long.toString(partitionVal)));
				request.addExpressionAttributeValuesEntry(":startJ", new AttributeValue().withN(Long.toString(j)));
				request.addExpressionAttributeValuesEntry(":endJ", new AttributeValue().withN(Long.toString(j + interval * 2-1)));
				request.setTableName(tableName);
				request.setKeyConditionExpression(condition);
				requests.add(request);
			}
			if (requests.size() > MAX_RANGES) {
				return null;
			}

			rangeCnt = requests.size();
			
			long ctr = 0;
			sw.start();
			for(QueryRequest request : requests){
				QueryResult result = operations.getClient().query(request);
			
				Iterator<Map<String, AttributeValue>> firstIter = result.getItems().iterator();
				while (firstIter.hasNext()) {
					firstIter.next();
					ctr++;
				}
				
				while((result.getLastEvaluatedKey() != null) && !result.getLastEvaluatedKey().isEmpty()){
					request.setExclusiveStartKey(result.getLastEvaluatedKey());
					result = operations.getClient().query(request);
					
					Iterator<Map<String, AttributeValue>> it = result.getItems().iterator();
					
					while (it.hasNext()) {
						it.next();
						ctr++;
					}
					
				}				
			}
			
			sw.stop();

			
			if (ctr != expectedResults) {
				System.err.println(
						"ERROR: experimentSkipScan, Interval is " + interval + " Count is " + ctr + " Expected " + expectedResults);
			}
			scanResults[i] = sw.getTime();
	
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	}

	private static Statistics oneRange(
			final DynamoDBOperations operations,
			final long cnt){
		double[] scanResults = new double[SAMPLE_SIZE];
		long rangeCnt = 0;
		long expectedResults = cnt;
		for (int i = 0; i < SAMPLE_SIZE; i++) {
			final StopWatch sw = new StopWatch();

			
			List<QueryRequest> requests = new ArrayList<QueryRequest>();
			long start = (TOTAL * 2 - cnt * 2) / 2L;
			
			String condition = new String(partitionKeyName + "= :val  AND " +
				    sortKeyName + " BETWEEN :startJ AND :endJ" );
			
			QueryRequest queryRequest = new QueryRequest();
			queryRequest.addExpressionAttributeValuesEntry(":val", new AttributeValue().withN(Long.toString(partitionVal)));
			queryRequest.addExpressionAttributeValuesEntry(":startJ", new AttributeValue().withN(Long.toString(start)));
			queryRequest.addExpressionAttributeValuesEntry(":endJ", new AttributeValue().withN(Long.toString(start + cnt * 2-1)));
			queryRequest.setTableName(tableName);
			queryRequest.setKeyConditionExpression(condition);
			requests.add(queryRequest);
			
			rangeCnt = requests.size();
			long ctr = 0;
			sw.start();
			for(QueryRequest request : requests){
				QueryResult result = operations.getClient().query(request);
				
				Iterator<Map<String, AttributeValue>> firstIter = result.getItems().iterator();
				while (firstIter.hasNext()) {
					firstIter.next();
					ctr++;
				}
				
				
				while((result.getLastEvaluatedKey() != null) && !result.getLastEvaluatedKey().isEmpty()){
					request.setExclusiveStartKey(result.getLastEvaluatedKey());
					result = operations.getClient().query(request);
					
					Iterator<Map<String, AttributeValue>> it = result.getItems().iterator();

					while (it.hasNext()) {
						it.next();
						ctr++;
					}
						
				}				
			}
			
			sw.stop();

			if (ctr != cnt) {
				System.err.println(
						"ERROR: extraData. Expected count is " + cnt + " Count got " + ctr);
			}
			scanResults[i] = sw.getTime();
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	}
}
