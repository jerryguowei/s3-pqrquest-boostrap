package org.example;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.example.api.RetryTask;
import org.example.api.ZipFileService;
import org.example.exceptions.RequireRetryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultZipFileService implements ZipFileService {
	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public List<String> unZipFile(File file) {
		RetryTask<List<String>> task = () -> { //Start Retry function
			List<String> fileNameList = new ArrayList<>();
			try (ZipInputStream zipInput = new ZipInputStream(new BufferedInputStream(new FileInputStream(file)))) {
				ZipEntry ze = null;
				byte[] buff = new byte[1024 * 8];
				int len = 0;
				while ((ze = zipInput.getNextEntry()) != null) {
					File outputFile = new File(Constants.TEMP_FOLDER + ze.getName());
					try (BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(outputFile))) {
						while ((len = zipInput.read(buff)) > 0)
							fos.write(buff, 0, len);
					}
					fileNameList.add(outputFile.getAbsolutePath());
				}
				return fileNameList;
			} catch (FileNotFoundException e) {
				logger.error(e.getMessage(), e);
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
			throw new RequireRetryException();
		};
		return RetryTask.runWithRetries(Constants.RETRY_TIMES, task);
	}
}
