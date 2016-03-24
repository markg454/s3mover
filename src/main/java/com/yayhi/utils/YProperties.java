package com.yayhi.utils;

import java.io.InputStream;
import java.net.InetAddress;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 * Looks up a resource named 'name' in the classpath. The resource must map
 * to a file with .properties extention. The name is assumed to be absolute
 * and can use either "/" or "." for package segment separation with an
 * optional leading "/" and optional ".properties" suffix. Thus, the
 * following names refer to the same resource:
 * <pre>
 * some.pkg.Resource
 * some.pkg.Resource.properties
 * some/pkg/Resource
 * some/pkg/Resource.properties
 * /some/pkg/Resource
 * /some/pkg/Resource.properties
 * </pre>
 * 
 * @param name classpath resource name [may not be null]
 * @param loader classloader through which to load the resource [null
 * is equivalent to the application loader]
 * 
 * @return resource converted to java.util.Properties [may be null if the
 * resource was not found and THROW_ON_LOAD_FAILURE is false]
 * @throws IllegalArgumentException if the resource was not found and
 * THROW_ON_LOAD_FAILURE is true
 */

public class YProperties {

    // private variables
    private static final boolean THROW_ON_LOAD_FAILURE 		= true;
    private static final boolean LOAD_AS_RESOURCE_BUNDLE 	= false;
    private static final String SUFFIX 						= ".properties";
    private String appPropertyFileName 						= "";
    private static final boolean debug						= false;
    
    public YProperties() {	
    	this.setPropertyFileName();
    }
    
    // set the property file name
    public void setPropertyFileName() {
    	
    	InetAddress localMachine 		= null;
        String hostFileName 			= "";
        
    	// get the local host name which is used dynamically build a properties file name
   		try	{
   			localMachine = InetAddress.getLocalHost();	
   		} catch(java.net.UnknownHostException uhe)
   		{
   			//handle exception
   		}
 
        //*********************************************************************************************
        //* Read the appropriate properties file
        //*********************************************************************************************
    	
   		// get the relative host name from the absolute host name
   		if (localMachine.getHostName().matches("^([a-zA-Z0-9_-]+)([\\.a-zA-Z0-9_-]+)*$")) {
   			
   			// local host name is the string to the left of the first period
   			String [] hostFields = localMachine.getHostName().split("\\.");
   			hostFileName = hostFields[0];
   			if (debug) {
   				System.out.println("YProperties: host field [0]: " + hostFields[0]);
   			}
   		}
   		else {
   			hostFileName = localMachine.getHostName(); // no period in the local host name
   		}

   		appPropertyFileName = "s3mover_" + hostFileName + ".properties";
   		if (debug) {
			System.out.println("YProperties: appPropertyFileName: " + appPropertyFileName);
		}
    	
    }
    
    // get the property file name
    public String getPropertyFileName() {
    	return appPropertyFileName;
    }
    
    // load the properties
    public Properties loadProperties(ClassLoader loader) {
    	
        if (appPropertyFileName == null)
            throw new IllegalArgumentException ("null input: " + appPropertyFileName);
        
        if (appPropertyFileName.startsWith ("/"))
        	appPropertyFileName = appPropertyFileName.substring (1);
            
        if (appPropertyFileName.endsWith (SUFFIX))
        	appPropertyFileName = appPropertyFileName.substring (0, appPropertyFileName.length () - SUFFIX.length ()); 	
        
        Properties result = null;
        
        InputStream in = null;
        try
        {
            if (loader == null) loader = ClassLoader.getSystemClassLoader ();
            
            if (LOAD_AS_RESOURCE_BUNDLE)
            {     
            	appPropertyFileName = appPropertyFileName.replace ('/', '.');
                // Throws MissingResourceException on lookup failures:
                final ResourceBundle rb = ResourceBundle.getBundle (appPropertyFileName,
                    Locale.getDefault (), loader);
                
                result = new Properties ();
                for (Enumeration<String> keys = rb.getKeys (); keys.hasMoreElements ();)
                {
                    final String key = (String) keys.nextElement ();
                    final String value = rb.getString (key);
                    
                    result.put (key, value);
                } 
            }
            else
            {
            	appPropertyFileName = appPropertyFileName.replace ('.', '/');
                
                if (! appPropertyFileName.endsWith (SUFFIX))
                	appPropertyFileName = appPropertyFileName.concat (SUFFIX);

                // Returns null on lookup failures:
                in = loader.getResourceAsStream (appPropertyFileName);

                if (in != null)
                {
                    result = new Properties ();
                    result.load (in); // Can throw IOException
                }
            }
        }
        catch (Exception e)
        {
            result = null;
        }
        finally
        {
            if (in != null) try { in.close (); } catch (Throwable ignore) {}
        }
        
        if (THROW_ON_LOAD_FAILURE && (result == null))
        {
            throw new IllegalArgumentException ("could not load [" + appPropertyFileName + "]"+
                " as " + (LOAD_AS_RESOURCE_BUNDLE
                ? "a resource bundle"
                : "a classloader resource"));
        }
        
        return result;
    }
    
    /**
     * A convenience overload of {@link #loadProperties(String, ClassLoader)}
     * that uses the current thread's context classloader.
     */
    public Properties loadProperties () {
        return loadProperties (Thread.currentThread().getContextClassLoader());
    }
    
} // End of class
