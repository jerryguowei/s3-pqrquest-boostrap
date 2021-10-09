package org.example;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.example.api.ParquetWriterService;
import org.example.api.S3Service;
import org.example.api.ZipFileService;
import org.example.exceptions.RetryFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3ToParquetFilter {
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	private S3Service s3Service;
	private ParquetWriterService parquetWriterService;
	private ZipFileService zipFileService;
	
	public S3ToParquetFilter() {
		s3Service = new DefaultS3Service();
		parquetWriterService = new DefaultParquetWriter();
		zipFileService = new DefaultZipFileService();
	}
	
	public void s3ToParquet(String bucketName) {
		try {
			List<String> csvFilePathList = getUnzipFiles(bucketName);			
			List<String> parquetFilePathList = convertToParquetFormat(csvFilePathList);
			uploadParquetFilesToS3(bucketName, parquetFilePathList);
			
		} catch (RetryFailedException e) {
			logger.error("failed after retry.");
			//put a message to message Queue for further process.
		} catch (Exception e) {
			logger.error("unckecked error", e);
			//put a message to message Queue for further process.
		}
	}

	private List<String> convertToParquetFormat(List<String> csvFilePathList) {
		List<String> parquetFilePathList = new ArrayList<>();
		for (String fullCsvFilePath : csvFilePathList) {
			String parquetFilePath = parquetWriterService.convertToParquet(new File(fullCsvFilePath));
			if (parquetFilePath != null)
				parquetFilePathList.add(parquetFilePath);
		}
		return parquetFilePathList;
	}

	private List<String> getUnzipFiles(String bucketName) {
		List<String> downloadFileList = s3Service.downloadZipFiles(bucketName);
		
		List<String> csvFilePathList = new ArrayList<>();
		for (String fullFilePath : downloadFileList) {
			List<String> tempcsvFilePathList = zipFileService.unZipFile(new File(fullFilePath));
			if (tempcsvFilePathList != null && !tempcsvFilePathList.isEmpty()) {
				csvFilePathList.addAll(tempcsvFilePathList);
			}
		}
		return csvFilePathList;
	}
	
	private void uploadParquetFilesToS3(String bucketName, List<String> parquetFilePathList) {
		for (String parquetFilePath : parquetFilePathList) {
			s3Service.uploadObject(bucketName, parquetFilePath,
					Utils.removeUUID(FilenameUtils.getName(parquetFilePath)));
		}
	}

	public S3Service getS3Service() {
		return s3Service;
	}

	public void setS3Service(S3Service s3Service) {
		this.s3Service = s3Service;
	}

	public void setZipFileService(ZipFileService zipFileService) {
		this.zipFileService = zipFileService;
	}

	public void setParquetWriterService(ParquetWriterService parquetWriterService) {
		this.parquetWriterService = parquetWriterService;
	}
}
