package com.yayhi.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class YUtils {

	static boolean debug = false;
	
	public YUtils () {
		
	}
    
    // replace a character at a specific position
    public static String replaceCharAt(String s, int pos, char c) {
    	
    	StringBuffer buf = new StringBuffer( s );
    	buf.setCharAt( pos, c );
    	return buf.toString( );
    	
    }
    
    public static Date convertStringToDate(String dateStr, String format) {

    	// Load formats
    	//ArrayList <SimpleDateFormat>dateFormats = loadFormats();
    	Date myDate = null;

//    	SimpleDateFormat myFormat = new SimpleDateFormat(format);
//
//		try {
//			myDate = (Date)myFormat.parse(dateStr);  
//		}
//		catch (ParseException e) {
//			e.printStackTrace();
//		}

		DateFormat df = new SimpleDateFormat(format);

	    try {
	        myDate = df.parse(dateStr);
	    } catch (ParseException e) {
	        e.printStackTrace();
	    }
	    
    	return myDate; 	 

    }
    
    // make a zip file
	public static File makeZipFile(String pathName, String entryName) throws IOException {
		
		ZipOutputStream zos = null;
		FileOutputStream fos = null;
		String zipFileName = pathName + ".zip";
		
		//System.out.println("YUtils: makeZipFile: pathName" + pathName);
		//System.out.println("YUtils: makeZipFile: entryName" + entryName);
		//System.out.println("YUtils: makeZipFile: zipFileName" + zipFileName);
		
		File zFile = new File(zipFileName);
		zFile.createNewFile();
		  
		byte[] buffer = new byte[1024];
		
		try{
			 
    		fos = new FileOutputStream(zipFileName);
    		zos = new ZipOutputStream(fos);
    		
    		ZipEntry ze = new ZipEntry(entryName);
    		zos.putNextEntry(ze);
    		
    		FileInputStream in = new FileInputStream(pathName);
 
    		int len;
    		while ((len = in.read(buffer)) > 0) {
    			zos.write(buffer, 0, len);
    		}
 
    		in.close();
    		zos.closeEntry();
    		zos.close();
 
    	} catch(IOException ex){
    	   ex.printStackTrace();
    	}  
		
		return zFile;
	 
	}
    
    // Send email notifications
    public static void emailNotification(String subject, String toAddress, String body) {
    	
    	String emailServer				= "localhost";
		String emailFromAddress			= "ingest@libredigital.com";
		String emailBody			    = "";
		
    	YMail acsi = new YMail();
    	
    	emailBody += body;
    	
    	// send email notification
     	try {
     		acsi.sendMail(emailServer,emailFromAddress,toAddress,subject,emailBody);
    	} catch (Exception e) {
			System.out.println(e);
		}
    	
    }
    
 // Send email notifications with an attachment
    public static void emailNotification(String subject, String toAddress, String body, File attachment) {
    	
    	String emailServer				= "localhost";
		String emailFromAddress			= "ingest@libredigital.com";
		String emailBody			    = "";
		
    	YMail acsi = new YMail();
    	
    	emailBody += body;
    	
    	// send email notification
     	try {
     		acsi.sendMail(emailServer,emailFromAddress,toAddress,subject,emailBody,attachment);
    	} catch (Exception e) {
			System.out.println(e);
		}
    	
    }	
    
    public static boolean isDate(String date) {

	    // Load formats
	    ArrayList <SimpleDateFormat>dateFormats = loadFormats();
	
	    boolean validDate = false;
	    Date myDate = null;
	
	    Object[] myDateFormats = dateFormats.toArray();
	    
	    for (int i = 0; i < dateFormats.size(); i++) {
	    
	    	SimpleDateFormat myFormat = (SimpleDateFormat) myDateFormats[i];
	   
	    	try {
	    		myFormat.setLenient(false);
	    		myDate = myFormat.parse(date);
	    		validDate = true;
	    		break;
	    	}
	    	catch (Exception e) {
	    		validDate = false;
	    	}
	    }
	    
	    return validDate;
    }
    
    public static ArrayList <SimpleDateFormat>loadFormats() {

    	ArrayList <SimpleDateFormat>dateFormats = new ArrayList<SimpleDateFormat>();
    	dateFormats.add(new SimpleDateFormat("yyyymmdd"));
    	dateFormats.add(new SimpleDateFormat("yyyy-MM-dd"));
    	dateFormats.add(new SimpleDateFormat("yyyy MM dd"));
    	dateFormats.add(new SimpleDateFormat("yyyy.MM.dd"));
    	dateFormats.add(new SimpleDateFormat("yyyy-MMM-dd"));
    	dateFormats.add(new SimpleDateFormat("yyyy MMM dd"));
    	dateFormats.add(new SimpleDateFormat("yyyy.MMM.dd"));
    	dateFormats.add(new SimpleDateFormat("dd-MM-yyyy"));
    	dateFormats.add(new SimpleDateFormat("dd MM yyyy"));
    	dateFormats.add(new SimpleDateFormat("dd.MM.yyyy"));
    	dateFormats.add(new SimpleDateFormat("dd/MM/yyyy"));
    	dateFormats.add(new SimpleDateFormat("dd-MM-yy"));
    	dateFormats.add(new SimpleDateFormat("dd MM yy"));
    	dateFormats.add(new SimpleDateFormat("dd.MM.yy"));
    	dateFormats.add(new SimpleDateFormat("dd/MM/yy"));
    	dateFormats.add(new SimpleDateFormat("dd-MMM-yy"));
    	dateFormats.add(new SimpleDateFormat("dd MMM yy"));
    	dateFormats.add(new SimpleDateFormat("dd.MMM.yy"));
    	dateFormats.add(new SimpleDateFormat("dd/MMM/yy"));
    	dateFormats.add(new SimpleDateFormat("dd-MMM-yyyy"));
    	dateFormats.add(new SimpleDateFormat("dd MMM yyyy"));
    	dateFormats.add(new SimpleDateFormat("dd.MMM.yyyy"));
    	dateFormats.add(new SimpleDateFormat("dd/MMM/yyyy"));
    	dateFormats.add(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"));
    	dateFormats.add(new SimpleDateFormat("yyyy MM dd hh:mm:ss"));
    	dateFormats.add(new SimpleDateFormat("yyyy.MM.dd hh:mm:ss"));
    	dateFormats.add(new SimpleDateFormat("yyyy/MM/dd hh:mm:ss"));
    	dateFormats.add(new SimpleDateFormat("yyyy-MMM-dd hh:mm:ss"));
    	dateFormats.add(new SimpleDateFormat("yyyy MMM dd hh:mm:ss"));
    	dateFormats.add(new SimpleDateFormat("yyyy.MMM.dd hh:mm:ss"));
    	dateFormats.add(new SimpleDateFormat("yyyy/MMM/dd hh:mm:ss"));
    	dateFormats.add(new SimpleDateFormat("dd-MM-yyyy hh:mm:ss"));
    	dateFormats.add(new SimpleDateFormat("dd MM yyyy hh:mm:ss"));
    	dateFormats.add(new SimpleDateFormat("dd.MM.yyyy hh:mm:ss"));
    	dateFormats.add(new SimpleDateFormat("dd/MM/yyyy hh:mm:ss"));
    	dateFormats.add(new SimpleDateFormat("dd-MMM-yyyy hh:mm:ss"));
    	dateFormats.add(new SimpleDateFormat("dd MMM yyyy hh:mm:ss"));
    	dateFormats.add(new SimpleDateFormat("dd.MMM.yyyy hh:mm:ss"));
    	dateFormats.add(new SimpleDateFormat("dd/MMM/yyyy hh:mm:ss"));

    	return dateFormats;
    	
    }
    
}
