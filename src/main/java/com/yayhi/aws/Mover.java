package com.yayhi.aws;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.ListIterator;
import java.util.Properties;

import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import com.yayhi.utils.YLogger;
import com.yayhi.utils.YProperties;

/**
 * -----------------------------------------------------------------------------
 * @version 1.0
 * @author  Mark Gaither
 * @date	Dec 3, 2009
 * -----------------------------------------------------------------------------
 */

public class Mover {

	private static Properties sysProps				= null;
	private static YProperties iProps				= null;
	private static Properties appProps				= null;
    private static String logFilePath				= null;
    private static Boolean logIt					= null;
    private static String inputFilePath				= null;
    private static String onlinePath				= null;
    private static boolean debug 					= false;
    private static YLogger logger					= null;
    private static String awsAccessKey				= null;
    private static String awsSecretKey				= null;
    private static S3Service s3Service				= null;
    private static AWSCredentials awsCredentials	= null;
    private static S3Bucket lsBucket				= null;
    private static S3Bucket assetBucket				= null;
    private static String lsBucketName				= null;
    private static String sourceDirStr				= null;
    
    
    // constructor
    Mover() {
    	
    }
    
    /**
     * Sole entry point to the class and application.
     * @param args Array of String arguments.
     * @exception java.lang.InterruptedException
     *            Thrown from the Thread class.
     * @throws IOException 
     */
    public static void main(String[] args) throws Exception {
    	
    	//*********************************************************************************************
        //* Get Command Line Arguments - overwrites the properties file value, if any
        //*********************************************************************************************   	
    	String usage = "Usage:\n" + "java -jar s3mover.jar [-i INPUT_MANIFEST] [-s SOURCE_ASSET_DIRECTORY] [-d] (DEBUG optional)\n";
    	String example = "Example:\n" + "java -jar s3mover.jar -i /tmp/mover.csv -s /tmp/images\n" +
    	"java -jar s3mover.jar -i /tmp/mover.txt -s /tmp/images -d\n";
    	
    	// get system properties
    	sysProps = System.getProperties();

        // get command line arguments
    	if (args.length >= 4) {
    		
	    	for (int optind = 0; optind < args.length; optind++) {
	    	    
	    		if (args[optind].equals("-i")) {
	    			inputFilePath = args[++optind];
				} else if (args[optind].equals("-s")) {
	    			sourceDirStr = args[++optind];
				} else if (args[optind].equals("-d")) {
					debug = true; 
		    	}
	    		
	    	}
        }
        else {
        	
        	System.err.println(usage);
        	System.err.println(example);
            System.exit(1);
            
        }

    	//*********************************************************************************************
        //* Get Properties File Data
        //*********************************************************************************************
    	iProps = new YProperties();
    	appProps = iProps.loadProperties();
    	
    	// set log file path
    	logFilePath = appProps.getProperty("logFilePath");
    	logIt = Boolean.valueOf(appProps.getProperty("logIt"));
    	awsAccessKey = appProps.getProperty("awsAccessKey");
    	awsSecretKey = appProps.getProperty("awsSecretKey");
    	lsBucketName = appProps.getProperty("awsBucket");
    	
    	//*********************************************************************************************
        //* Set up AWS S3 services
        //*********************************************************************************************
    	// get AWS credentials
    	awsCredentials = new AWSCredentials(awsAccessKey,awsSecretKey);
    	
    	if (debug) {
    		System.out.println("awsAccessKey: " + awsAccessKey);
    		System.out.println("awsSecretKey: " + awsSecretKey);
    		System.out.println("lsBucket: " + lsBucketName);
    		System.out.println("sourceDirStr: " + sourceDirStr);
    	}
    	
    	// To communicate with S3, create a class that implements an S3Service. 
    	try {
    		s3Service = new RestS3Service(awsCredentials);
    	}
    	catch (S3ServiceException s) {
    		s.printStackTrace();
    	}
        
        // get Livestrong bucket
        try {
	        lsBucket = s3Service.getOrCreateBucket(lsBucketName);
        }
        catch (ServiceException s) {
        	s.printStackTrace();
        }
        
    	//*********************************************************************************************
        //* Set up logging
        //*********************************************************************************************

    	// create string of todays date and time
    	Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_hhmmss");

        // log data, if true
        if (logIt.booleanValue()) {
            	
        	// open log file
    	    try {
    	    	logger = new YLogger(appProps.getProperty("logFilePath") +  "/s3mover_" + sdf.format(cal.getTime()) + ".log");
    	    } catch (IOException e) {
    	    	System.out.println("exception: " + e.getMessage());
    	    }
    	    
        } 
        
        if (debug) {
    		System.out.println("   property log path :  " + appProps.getProperty("logFilePath") + "/s3mover_" + sdf.format(cal.getTime()) + ".log");
    		System.out.println("   property log it :  " + logIt.toString());
    		System.out.println("   manifest file path :  " + inputFilePath);
        }
        
        //*********************************************************************************************
        //* Read input file of local image paths
        //*********************************************************************************************
    	
    	// Create the manifest source file
		File manifest = new File(inputFilePath);

		// Save input asset path
		ArrayList <String>assetsList = new ArrayList<String>();

		// Read manifest file
		if (manifest.exists()) {
			
			if (debug) {
				System.out.println("Processing manifest: " + inputFilePath);
				System.out.println("\n===============================================");
	        }
		    
			if (logIt.booleanValue()) {
      			
      			try {
      				logger.write("Processing manifest: " + inputFilePath);
      				logger.write("\n===============================================");
          	    } catch (IOException e) {
          	    }
          	    
      		}
			
			// read the manifest 
			// each asset path is on a line to itself
		    try {

		    	BufferedReader input =  new BufferedReader(new FileReader(inputFilePath));
  
		    	try {
    
		    		String line = null; //not declared within while loop

		    		while (( line = input.readLine()) != null) {

		    			if (debug) {
		    				System.out.println("   line: " + line);
		    			}
		    			
		    			// save beid to array list
		    			assetsList.add(line);
	
		    		}
		    	}
		    	finally {
		    		input.close();
		    	}
		    }
		    catch (IOException ex){
		    	ex.printStackTrace();
		    }
		    
		    // for each beid, copy the ebook to the cloud
		    ListIterator <String>assetItr = assetsList.listIterator();
		    
		    if (debug) {
		    	System.out.println("   assetsList length: " + assetsList.size());
				System.out.println("   assetItr.hasNext(): " + assetItr.hasNext());
			}
		    
		    //*********************************************************************************************
	        //* Move each asset to S3
	        //*********************************************************************************************
		    while (assetItr.hasNext()) {
	        	
	        	// get next path 
	        	String path = (String) assetItr.next();
	        	
	        	// build full path to file
	        	String assetPath = sourceDirStr + "/" + path;
	        	
	        	// create full bucket name base on path parts
	        	String [] parts;
	        	String delimeter = "/";
	        	parts = path.split(delimeter);
	        	
	        	String objectName = "";
	        	for (String part: parts) {
	        		if (objectName.equals("")) {
	        			objectName = part.replace(" ","+");
	        		} else{
	        			objectName += "/" + part.replace(" ","+");
	        		}	
	        	}
	        	
	        	if (debug) {
	        		System.out.println(" path: " + path);
	        		System.out.println(" asset path: " + assetPath);
	        		for (String retval: path.split("/")) {
	        			System.out.println("part of parts: " + retval);
	        	    }
	        		System.out.println(" asset object name: " + objectName);
	        	}
	        	
	        	// get LS bucket
	        	try {
	        		lsBucket = s3Service.getBucket(lsBucketName);
	        	}
	        	catch (Exception e) {
	        		e.printStackTrace();
	        	}
	        	
	        	//*********************************************************************************
	        	// Create an S3Object based on a file, with Content-Length set automatically and 
	            // Content-Type set based on the file's extension
	        	//*********************************************************************************
	            File fileData = new File(assetPath);
	            
	            if (fileData.exists()) {	
	            	
	            	S3Object fileObject = new S3Object(objectName);
	            	fileObject.setBucketName(lsBucketName);
		        	fileObject.setContentLength(fileData.length());
		        	fileObject.setDataInputFile(fileData);
		        	
		            // put the object into the bucket
		            s3Service.putObject(lsBucket, fileObject);

		            // add upload date and time metadata
		            fileObject.addMetadata(S3Object.METADATA_HEADER_LAST_MODIFIED_DATE,cal.getTime());		        	
		        	
		            System.out.println();
			        System.out.println("   --------------- Copy COMPLETE for " + assetPath);
			        System.out.println();
			        
			        try {
	      				logger.write("\n\t--------------- Copy COMPLETE for " + assetPath);
	          	    } catch (IOException e) {
	          	    }
	          	    
	            }
	            else {

	            	System.err.println("Unable to retrieve file data for " + assetPath);
	                System.exit(1);
	            	
	            }        
	         	
	        	
		    }
		
		}
		
		logger.close();
  	    
        System.exit(0);
		
    }
    
}
