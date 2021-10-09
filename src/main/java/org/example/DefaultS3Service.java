package org.example;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.example.api.RetryTask;
import org.example.api.S3Service;
import org.example.exceptions.RequireRetryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class DefaultS3Service implements S3Service {

	private Logger logger = LoggerFactory.getLogger(this.getClass());
	private final AmazonS3 s3;

	public DefaultS3Service() {
		s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.AP_SOUTHEAST_2)
				.build();
	}
	
	@Override
	public List<String> downloadZipFiles(String bucketName) {
		RetryTask<List<String>> task = () -> {
			List<String> fileNameList = new ArrayList<>();
			ListObjectsV2Result result = s3.listObjectsV2(bucketName);
			List<S3ObjectSummary> objects = result.getObjectSummaries();
			for (S3ObjectSummary os : objects) {
				if (!os.getKey().endsWith(".zip"))
					continue;
				logger.info(os.getKey());
				String fileFullPath = downloadSingleFile(os.getKey(), bucketName);
				if (fileFullPath != null)
					fileNameList.add(fileFullPath);
			}
			if (fileNameList.isEmpty())
				throw new RequireRetryException();
			return fileNameList;
		};
		return RetryTask.runWithRetries(Constants.RETRY_TIMES, task);
	}

	private String downloadSingleFile(String keyName, String bucketName) {

		RetryTask<String> task = () -> {
			S3Object s3Object = s3.getObject(bucketName, keyName);
			createTmpFolderIfNotExit();
			File file = new File(Constants.TEMP_FOLDER + keyName);
			try (S3ObjectInputStream s3InputSteram = s3Object.getObjectContent();
					BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(file))) {

				byte[] buff = new byte[1024 * 8];
				int len = 0;
				while ((len = s3InputSteram.read(buff)) > 0) {
					output.write(buff, 0, len);
				}
				return file.getAbsolutePath();
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
			throw new RequireRetryException();
		};
		return RetryTask.runWithRetries(Constants.RETRY_TIMES, task);
	}

	private void createTmpFolderIfNotExit() {
		File directory = new File(Constants.TEMP_FOLDER);
		if (!directory.exists()) {
			directory.mkdir();
		}
	}

	@Override
	public boolean uploadObject(String bucketName, String filePath, String keyName) {
		RetryTask<Boolean> task = () -> {
			try {
				s3.putObject(bucketName, keyName, new File(filePath));
				return true;
			} catch (SdkClientException e) {
				logger.error(e.getMessage());
				throw new RequireRetryException();
			}
		};
		return RetryTask.runWithRetries(Constants.RETRY_TIMES, task);
	}

	@Override
	public boolean isObjectExist(String bucketName, String keyName) {
		return s3.doesObjectExist(bucketName, keyName);
	}
}
