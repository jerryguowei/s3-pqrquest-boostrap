package org.example;

import java.util.UUID;

public class Utils {
	public static String getUUID() {
		return UUID.randomUUID().toString();
	}
	
	public static String removeUUID(String fileName) {
		if(fileName != null && fileName.length() > 37) {
			fileName = fileName.substring(37);
		}
		return fileName;
	}
}
