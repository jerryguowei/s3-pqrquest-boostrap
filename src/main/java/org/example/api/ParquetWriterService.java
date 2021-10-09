package org.example.api;

import java.io.File;

public interface ParquetWriterService {
	public String convertToParquet(File csvFile);
}
