package com.yayhi.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

public class YLogger {

	// private variables
	private String filePath							= null;
	private FileOutputStream fileOutputStream		= null;
	private File outputFile							= null;
	
	public YLogger(String path) throws IOException {
		
		filePath = path;
		
		outputFile = new File(filePath);
		
		// open the output file stream
	    try {
	    	fileOutputStream = new FileOutputStream(filePath);
	    } catch (IOException e) {
	      System.out.println(e);
	    }
	    
	}
	
	// get file path
	public String getFilePath() {
		return filePath;
	}
	
	// set file path
	public void setFilePath(String path) {
		filePath = path;
	}
	
	// get file output stream
	public FileOutputStream getFileOutputStream() {
		return fileOutputStream;
	}
	
	// set file output stream
	public void setFileOutputStream(FileOutputStream fos) {
		fileOutputStream = fos;
	}
	
	// get output file 
	public File getOutputFile() {
		return outputFile;
	}
	
	// set file output 
	public void setOutputFile(File f) {
		outputFile = f;
	}
	
    // close the output stream
    public void close() {
    	
    	try {
	    	fileOutputStream.flush();
    	} catch (IOException e) {
    		System.out.println(e);
    	}
    	
    	try {
	    	fileOutputStream.close();
    	} catch (IOException e) {
    		System.out.println(e);
    	}
    	
    }
    
    // write a line to the file
    public void write(String data) throws IOException {
    	
    	data = data + "\n";
    	
	    if(outputFile.exists()){
	    	fileOutputStream.write(data.getBytes());
	    }
	    
    }
    
    // read all lines from the file
    public String getLines() throws IOException {
    	
    	String lines = "";
    	
    	try {

    	    BufferedReader in = new BufferedReader(new FileReader(filePath));
    	    
    	    String line;
    	    while ((line = in.readLine()) != null)
    	      lines += line + "\n";
    	    
    	    in.close();
		
		} catch (FileNotFoundException e) {
		  e.printStackTrace();
		} catch (IOException e) {
		  e.printStackTrace();
		}
	    
		return lines;
		
    }
    
}
