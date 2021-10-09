package org.example.api;

import java.util.List;

public interface S3Service {
	/**
	 * Download zip files from the named bucket.
	 * @param bucketName
	 * @return the full path of download zip file in local disk.
	 */
	List<String> downloadZipFiles(String bucketName);
	
	boolean uploadObject(String bucketName, String filePath, String keyName);
	
	//for test
	boolean isObjectExist(String bucketName, String keyName);

}
