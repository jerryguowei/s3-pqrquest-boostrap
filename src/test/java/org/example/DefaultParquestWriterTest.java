package org.example;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.junit.jupiter.api.Test;

public class DefaultParquestWriterTest {
	
	@Test
	public void testConvertToFile() {
		File csvFile = new File(Constants.TEMP_FOLDER + "ausnews.csv");
		DefaultParquetWriter writer = new DefaultParquetWriter();
		String fileName = writer.convertToParquet(csvFile);
		File file  = new File(fileName);
		assertTrue(file.exists());
	}

}
