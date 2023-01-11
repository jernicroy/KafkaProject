package com.asirtech.producers.DTO;

import com.opencsv.bean.CsvBindByName;

public class StudentModel {
	@CsvBindByName
	private int reg_no;
	@CsvBindByName
	private String first_name;
	@CsvBindByName
	private String last_name;
	@CsvBindByName
	private String location;
	@CsvBindByName
	private String state;
	@CsvBindByName
	private String language;
	
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
	}
	public int getReg_no() {
		return reg_no;
	}
	public void setReg_no(int reg_no) {
		this.reg_no = reg_no;
	}
	public String getFirst_name() {
		return first_name;
	}
	public void setFirst_name(String first_name) {
		this.first_name = first_name;
	}
	public String getLast_name() {
		return last_name;
	}
	public void setLast_name(String last_name) {
		this.last_name = last_name;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
}
