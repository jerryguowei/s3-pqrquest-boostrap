package org.example;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class S3ToPrquetFilterTest {

	@Test
	public void testS3ToParquet() {
		S3ToParquetFilter s3ToParquetFilter = new S3ToParquetFilter();
		s3ToParquetFilter.s3ToParquet(Constants.TEST_BUCKET_NAME);
		//Test if the parquet file already upload to S3 bucket.
		assertTrue(s3ToParquetFilter.getS3Service().isObjectExist(Constants.TEST_BUCKET_NAME, "AirbnbListing.parquet"));
	}
}
