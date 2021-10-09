package org.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.util.List;

import org.example.api.ZipFileService;
import org.example.exceptions.RetryFailedException;
import org.junit.jupiter.api.Test;

public class DefaultZipFileServiceTest {
	
	@Test
	public void testUnZipFile() {
		File zipFile = new File(Constants.TEMP_FOLDER + "data.zip");
		
		ZipFileService zipFileService = new DefaultZipFileService();
		List<String> zippedFileNamelist = zipFileService.unZipFile(zipFile);
		System.out.println(zippedFileNamelist);
		assertEquals(3, zippedFileNamelist.size());
	}
		
	@Test
	public void testFailureRetry() {
		ZipFileService zipFileService = new DefaultZipFileService();
		assertThrows(RetryFailedException.class, () -> {
			zipFileService.unZipFile(new File("not-exist.zip"));
		});
	}
}
