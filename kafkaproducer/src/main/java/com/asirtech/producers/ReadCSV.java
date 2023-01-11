package com.asirtech.producers;

import java.io.FileReader;
import java.util.List;

import com.asirtech.producers.DTO.StudentModel;
import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

public class ReadCSV {
	private String csvFilename;
	private List<?> stdList;
	
	ReadCSV(String csvFilename){
		this.csvFilename=csvFilename;
	}
	public List<?> ReadCSVFile() {
		try {
			CSVReader csvReader = new CSVReader(new FileReader(csvFilename));
			CsvToBean<?> csvToBean = new CsvToBeanBuilder<Object>(csvReader)
					.withType(StudentModel.class)
					.withIgnoreLeadingWhiteSpace(true)
					.build();
			
			stdList = csvToBean.parse();
		}catch(Exception e) {
			System.out.println("File is not found...");
			e.printStackTrace();
		}
		return stdList;
	}
}
