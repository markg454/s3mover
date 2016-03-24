package com.yayhi.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.Ostermiller.util.CSVPrinter;

public class YCSVFileWriter {

	// private variables
	private String yCSVFileWriterFilePath			= null;
	private File yCSVFileWriterFile				= null;
	private String[] yCSVFileWriterColumns			= null;
	private String[] yCSVFileWriterRowData			= null;
	private FileOutputStream fos					= null;
	private CSVPrinter csvp							= null;
	
	public YCSVFileWriter(String path) {
		
		// set csv file path
		yCSVFileWriterFilePath = path;
		
		// create a file
		yCSVFileWriterFile = new File(yCSVFileWriterFilePath);
		
		// open the output file stream for writing
	    try {
	    	fos = new FileOutputStream(yCSVFileWriterFilePath);
	    } catch (IOException e) {
	      System.out.println(e);
	    }
	    
    	// create new CSV Printer
    	csvp = new CSVPrinter(fos);

	}
	
	// get file path
	public String getFilePath() {
		return yCSVFileWriterFilePath;
	}
	
	// set file path
	public void setFilePath(String path) {
		yCSVFileWriterFilePath = path;
	}
	
	// get file 
	public File getFile() {
		return yCSVFileWriterFile;
	}
	
	// set file 
	public void setFile(File f) {
		yCSVFileWriterFile = f;
	}
	
	// get columns
    public String[] getCSVFileWriterColumns() {
        return yCSVFileWriterColumns;
    }
    
    // set columns
    public void setCSVFileWriterColumns(String[] columnstrs) {
        yCSVFileWriterColumns = columnstrs;
    }
    
    // get row data
    public String[] getCSVFileWriterRowData() {
        return yCSVFileWriterRowData;
    }
    
    // set row data
    public void setCSVFileWriterRowData(String[] datastr) {
        yCSVFileWriterRowData = datastr;
    }
    
    // close the output stream
    public void close() {

    	try {
    		csvp.close();
    	} catch (IOException e) {
    		System.out.println(e);
    	}
    	
    	try {
    		fos.close();
    	} catch (IOException e) {
    		System.out.println(e);
    	}
    }
    
    // write a line to the CSV file
    public boolean write(String[] data) {
    	
    	boolean success = false;
    	
    	csvp.setAlwaysQuote(true);
    	
    	// write out the column headers
	    try {
	    	csvp.writeln(data);
	    	success = true;
	    } catch (IOException e) {
	    	System.out.println(e);
  	    }
    	
	    return success;
	    
    }
}
