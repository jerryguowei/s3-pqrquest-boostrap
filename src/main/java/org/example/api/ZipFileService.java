package org.example.api;

import java.io.File;
import java.util.List;

public interface ZipFileService {

	/**
	 * @param file zip file
	 * @return unziped full file path
	 */
	List<String> unZipFile(File file);
}
