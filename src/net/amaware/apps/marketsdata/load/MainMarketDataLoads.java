package net.amaware.apps.marketsdata.load;

//import net.amaware.main.mainexcel.DataProcess;
import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import org.apache.poi.ss.usermodel.Sheet;

import net.amaware.serv.HtmlTargetServ;
import net.amaware.serv.SourceProperty;
import net.amaware.app.DataStoreReport;
import net.amaware.app.MainAppDataStore;
import net.amaware.appsbase.datatrack.DataTrackAccess;
import net.amaware.autil.*;
//
/**
 * @author AMAWARE - Angelo M Adduci
 * 
 */
public class MainMarketDataLoads {
	final static String thisClassName = "MainMarketDataLoads";	
	// set Properties file to being used
    final static String thisPropFileName      = "MainMarketDataLoads.properties";
    final static String marketsPropFileName   = "markets-amaware.properties";
    //
    
    //
    static String argSqlFileDir = "";    
    static String argSqlDefaultFileName  = "SqlInput.txt";
    static String argSqlFileNameSansExt = "";
    static String argSqlFileExt = "";
    static String argFileFullName = "";
	static String argSqlString = "";	
	//
	static boolean isSqlFromFile = false;	
    //    
	// init classes to be used
	//static ACommDb acomm = null;
	static ACommDbFile acomm;
	//
	static SourceProperty aSourceProperty = new SourceProperty();	
	//
	//Service Classes to process User classes
	//static SqlResultServ _sqlResultServ;

    //Target Services
	//static HtmlTargetServ aHtmlServReport;
//	static AFileExcelPOI aFileExcelPOI = new AFileExcelPOI(); 
//	Sheet aSheetDetail;	    
	//    	
	//Architecture Class
	static MainAppDataStore mainApp;
	//Application Classes
	//static FeedDataStoreReport thisDataStore = null;
	//
	//static EodFundamentalsDirFile eodFundamentalsDirFile = new EodFundamentalsDirFile();
    //
	public enum DirectoryName {  FUNDAMENTALS, NAMES, NONE; };
	static DirectoryName directoryName;
    //
	static String fileNameToProcess="";
	//
	
    //
	public static void main(String[] args) {
		//final String thisClassName = "MainExcelDbTable";
		//

		// 
		try { //setup the com class with properties file and log file prop key
			acomm = new ACommDbFile(thisPropFileName, args);
		
			acomm.addPageMsgsLineOut(thisClassName+"=>getArgFilePath{" + ACommDbFile.getArgFilePath() +"}"
					                              +" |ArgFilePathDirName{" + ACommDbFile.getArgFilePathDirName() +"}"
					                              );			
			
	    	switch (ACommDbFile.getArgFilePathDirName().toLowerCase()) {
              case "fundamentals": 
				   acomm.addPageMsgsLineOut("Processing for{"+ACommDbFile.getArgFilePathDirName()+"}");
         	       directoryName = DirectoryName.FUNDAMENTALS;
         	       
         	       EodDirectoryFundamentals aEodDirectoryFundamentals = new EodDirectoryFundamentals(acomm, marketsPropFileName);
         	       aEodDirectoryFundamentals.doProcess(acomm);
         	       //
         	       aEodDirectoryFundamentals.connectionEnd();
         	       //
         	       
                   break;

              case "names": 
            	   acomm.addPageMsgsLineOut("Processing for{"+ACommDbFile.getArgFilePathDirName()+"}");
         	       directoryName = DirectoryName.NAMES;
         	    
         	       EodDirectoryNames aEodDirectoryNames = new EodDirectoryNames(acomm, marketsPropFileName);
         	       aEodDirectoryNames.doProcess(acomm);
         	       //
         	       aEodDirectoryNames.connectionEnd();
         	       //
                   break;
                 
              default: 
		    	  //processFile(acomm, inFile);
		    	   String outmsg="===UNKNOWN Directory Name===>"+ACommDbFile.getArgFilePathDirName();
				   acomm.addPageMsgsLineOut(thisClassName+"===UNKNOWN Directory Name===>"+ACommDbFile.getArgFilePathDirName());
		    	  throw new AException(acomm, outmsg);	    	
	    	}
	    	//

	    	acomm.dbConClose();
	    	acomm.end();
			//
		} catch (AExceptionSql e1) {
			acomm.addPageMsgsLineOut("MainExcelDbLoad AExceptionSql msg{"+e1.getMessage()+e1.getExceptionMsg()+"}");
			throw e1;

		} catch (AException e1) {
			acomm.addPageMsgsLineOut("MainExcelDbLoad AException msg{"+e1.getMessage()+"}");
			throw e1;
		
			
		}

	}	
	//

//
// END CLASS
//	
}
