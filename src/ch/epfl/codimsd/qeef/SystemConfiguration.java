/*
* CoDIMS version 1.0 
* Copyright (C) 2006 Othman Tajmouati
*
* This library is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License as published by the Free Software Foundation; either
* version 2.1 of the License, or (at your option) any later version.
*
* This library is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this library; if not, write to the Free Software
* Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/
package ch.epfl.codimsd.qeef;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;

import org.apache.log4j.Logger;

import ch.epfl.codimsd.exceptions.initialization.InitialConfigurationException;
import ch.epfl.codimsd.qeef.util.Constants;

/**
 * The SystemConfiguration class keeps Codims parameters. It loads the codims.properties file.
 * and keep the values in a Hashtable.
 * 
 * @author Othman Tajmouati 
 *
 */
public class SystemConfiguration {

	/**
	 * Indicates if the system has been initialized. This is the case when 
	 * come components need to check the system state.
	 */
	private static boolean systemInitialized = false;
	
	/**
	 * Log4j logger.
	 */
	protected static Logger logger = Logger.getLogger(SystemConfiguration.class.getName());
	
	/**
	 * Keeps system configuraton values.
	 */
	private static Hashtable<String,String> systemConfigInfo;
	
	/**
	 * Codims-home directory location.
	 */
	private static String home = null;
	
	/**
	 * Load intial properties, put the codims-home and the CatalogIRI in the hashtable.
	 * The method assumes that codims-home is located under user directory. When 
	 * Codims interfact with other systems, users can specify another location
	 * by calling the setHome method before calling this method.
	 * 
	 * @throws InitialConfigurationException
	 */
	public static void loadSystemConfiguration() throws InitialConfigurationException {
		
		// Initializations & Put codims-home location in both the BlackBoard and the
		// Systemconfiguration hashtable.
                //home = "/home/rodrigo/NetBeansProjects/QEF_Hadoop";
           
            String homedir=System.getProperty("user.home");
            System.out.println(homedir);
            
            home = homedir+"/Project_QEF_1.0/ProjetoQEF_1.0"; 
          
		systemConfigInfo = new Hashtable<String,String>();
		systemConfigInfo.put(Constants.HOME, home);
		
		BlackBoard bl = BlackBoard.getBlackBoard();
		bl.put(Constants.HOME, home);
				
             
		systemInitialized = true;
	}
	
	public static boolean getSystemState() {
		return systemInitialized;
	}
	
	/**
	 * Overwrite the codims-home location if needed.
	 * @param home codims-home location.
	 */
	public static void setHome(String newHome) {
		home = newHome;
	}
	
	/**
	 * Return the value corresponding to this key from the System properties.
	 * 
	 * @param key the key.
	 * @return the associated value.
	 */
 	public static String getSystemProperty(String key) {
		return System.getProperty(key);
	}

 	/**
 	 * Return the value corresponding to this key from the system config hashtable.
 	 * 
 	 * @param key the key.
 	 * @return the associated value.
 	 */
	public static synchronized String getSystemConfigInfo(String key) {

		return systemConfigInfo.get(key);
	}
}

