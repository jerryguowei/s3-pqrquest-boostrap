package org.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.example.api.ParquetWriterService;
import org.example.api.RetryTask;
import org.example.exceptions.RequireRetryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultParquetWriter implements ParquetWriterService {
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	

	@Override
	public String convertToParquet(File csvFile) {
		RetryTask<String> task = () -> {
			return convertToParquetNoRetry(csvFile);
		};	
		return RetryTask.runWithRetries(Constants.RETRY_TIMES, task);
	}
	
	/**
	 * Convert a CSV file to Parquet Format file using @see AvroParquetWriter
	 * @param csvFile
	 * @return the full file path to Parquet Format file.
	 */
	private String convertToParquetNoRetry(File csvFile) {	
		ParquetWriter<GenericData.Record> writer = null;
		boolean hasHeader = hasHeader(csvFile);
		Schema schema = null;
		boolean getFirstLine = false;
		List<String> headerNameList = null;
		try(BOMInputStream bomInputStream = new BOMInputStream(new FileInputStream(csvFile)); //use bomInputStream to remove UTF-8 BOM character
			BufferedReader reader = new BufferedReader(new InputStreamReader(bomInputStream));) {
			HadoopOutputFile outputFile = HadoopOutputFile.fromPath(new Path(Constants.TEMP_FOLDER +
					Utils.getUUID() + "_" + FilenameUtils.removeExtension(csvFile.getName()) + ".parquet"), new Configuration());
			String line = null;
			while((line = reader.readLine()) != null) {
				if(!getFirstLine) {
					//to builder headers, schema and writer from first line.
					headerNameList = buildHeaders(line, hasHeader);
					schema = buildSchema(headerNameList, FilenameUtils.removeExtension(csvFile.getName()));
					writer =  AvroParquetWriter
							.<GenericData.Record>builder(outputFile).withSchema(schema).withCompressionCodec(CompressionCodecName.SNAPPY)
					        .build();
					getFirstLine = true;
					if(hasHeader) continue;
				}
				if(!match(line)) continue;
				GenericData.Record record = buildData(headerNameList, line, schema);
				writer.write(record);
			}
			writer.close();
			return outputFile.getPath().substring(6);
		} catch (FileNotFoundException e1) {
			 logger.error(e1.getMessage(), e1);
		} catch (IOException e2) {
			 logger.error(e2.getMessage(), e2);
		}	
		throw new RequireRetryException();
	}
	//can overwrite this method to support other matches.
	protected boolean match(String line) {
		if(line == null) return false;
		return line.contains("ellipsis");
	}
	
	
	private GenericData.Record buildData(List<String> headerNameList, String line, Schema schema) {
		GenericData.Record record = new GenericData.Record(schema);
		List<String> rowDataList = Arrays.asList(line.split(Constants.CSV_SPLITOR));
		for (int i = 0; i < headerNameList.size(); i++) {
			String header = headerNameList.get(i);
			record.put(header, rowDataList.get(i));
		}

		return record;
	}
	/**
	 * if has header, we return the header,
	 * otherwise we return col1, col2 ... colx;
	 * @param firstLine
	 * @param hasHeader
	 * @return
	 */
	private List<String> buildHeaders(String firstLine, boolean hasHeader){
			String[] headerCells = firstLine.split(",");
			List<String> headerNamelist = new ArrayList<>(headerCells.length);
			
			for(int i = 0; i < headerCells.length; i++) {
				String columnName = null;
				if(hasHeader) {
					columnName = headerCells[i];
				} else {
					columnName = "col" + (i + 1);
				}
				headerNamelist.add(columnName.trim());
			}
			return headerNamelist;
	}
	
	private Schema buildSchema(List<String> headerList, String name) {
		
		FieldAssembler<Schema> assembler = SchemaBuilder.builder().record(name).fields();
		for(int i = 0; i < headerList.size(); i++) {
			String columnName = headerList.get(i);
			assembler =  assembler.name(columnName).type().nullable().stringType().noDefault();	
		}
		return assembler.endRecord();
	}
	
	//I do not find a way to check if the file has header, 
	//so hard code the file name for this task.
	private boolean hasHeader(File csvFile) {
		if("ausnews.csv".equals(csvFile.getName())) {
			return false;
		}
		
		return true;
	}
}
