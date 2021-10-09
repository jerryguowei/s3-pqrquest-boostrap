package org.example;

import java.io.File;

public class Constants {
	public static String TEMP_FOLDER = "tmp" + File.separator;
	public static String CSV_SPLITOR = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
	public static final String UTF8_BOM = "\uFEFF";	
	public static int RETRY_TIMES = 5;
	public static final String TEST_BUCKET_NAME = "candidate-83-s3-bucket";
}
