package org.example;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

public class DefaultS3ServiceTest {

	@Test
	public void testdownloadZipFiles() {
		String bucketName = Constants.TEST_BUCKET_NAME;
		DefaultS3Service s3Service = new DefaultS3Service();
		List<String> fileNameList = s3Service.downloadZipFiles(bucketName);
		assertEquals(1, fileNameList.size());
	}
}
