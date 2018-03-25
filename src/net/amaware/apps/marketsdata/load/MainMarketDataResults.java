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
public class MainMarketDataResults {
	final static String thisClassName = "MainMarketDataResults";	
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
	public enum DirectoryName {  TECHNICAL, FUNDAMENTALS, NAMES, NONE; };
	static DirectoryName directoryName;
    //
	static String fileNameToProcess="";
	//
    //ADatabaseAccess thisADatabaseAccess; // in super
	static AFileExcelPOI aFileExcelPOI = new AFileExcelPOI(); 
	Sheet aSheetRequest;	 
	Sheet aSheetResult;    
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
	    	//
			ADatabaseAccess appADatabaseAccess = new ADatabaseAccess(acomm, marketsPropFileName, "data_track_store", true); 			
			//
		    String outExcelFileName=AComm.getOutFileDirectory()+AComm.getFileDirSeperator()+thisClassName+AComm.getAppClassFileSep()
	        +AComm.getArgFileName()+".report.xls";

	         acomm.addPageMsgsLineOut(thisClassName+ "=>Output Excel File Name{" +outExcelFileName +"}");
			//
	        aFileExcelPOI = new AFileExcelPOI(acomm, outExcelFileName);
	        //				
			appADatabaseAccess.doQueryRsExcel(aFileExcelPOI
					//thisDataTrackAccess.doQueryRsExcel(aFileExcelPOI
			                , "doQueryRsExcel data_track"
			                , "Select *"
			                  +" from data_track " 
			                 //+ " Where field_nme  = '" + ufieldname +"'" 
			                 //+ " order by tab_name"
			                 + " order by subject, topic, item"
			                 );
			appADatabaseAccess.doDbTabMetadataExcelSheet(aFileExcelPOI,"data_track");
			//
			appADatabaseAccess.doQueryRsExcel(aFileExcelPOI
					//thisDataTrackAccess.childDataTrackStoreAccess.doQueryRsExcel(aFileExcelPOI
			                , "doQueryRsExcel data_track_store"
			                , "Select *"
			                  +" from data_track_store " 
			                 //+ " Where source_nme  = '" + thisDataTrackAccess.getTrackFileName() +"'"
			                 //+ " order by tab_name"
			                 + " order by  run_start_ts desc, source_nme, source_mod_ts, data_track_id"
			                 );		        
	        //
			//
			appADatabaseAccess.doDbTabMetadataExcelSheet(aFileExcelPOI,"data_track_store");
	        //
			appADatabaseAccess.doQueryRsExcel(aFileExcelPOI
					//thisDataTrackAccess.childDataTrackStoreAccess.doQueryRsExcel(aFileExcelPOI
			                , "doQueryRsExcel ref_exchange"
			                , "Select *"
			                  +" from ref_exchange " 
			                 + " order by exch_cd"
			                 );		        
			appADatabaseAccess.doDbTabMetadataExcelSheet(aFileExcelPOI,"ref_exchange");			
	        //
			appADatabaseAccess.doQueryRsExcel(aFileExcelPOI
					//thisDataTrackAccess.childDataTrackStoreAccess.doQueryRsExcel(aFileExcelPOI
			                , "doQueryRsExcel ref_exch_symb"
			                , "Select *"
			                  +" from ref_exch_symb " 
			                 + " order by exch_cd, symb_cd"
			                 );		        
			appADatabaseAccess.doDbTabMetadataExcelSheet(aFileExcelPOI,"ref_exch_symb");
            //
			appADatabaseAccess.doQueryRsExcel(aFileExcelPOI
					//thisDataTrackAccess.childDataTrackStoreAccess.doQueryRsExcel(aFileExcelPOI
			                , "doQueryRsExcel ref_sector"
			                , "Select *"
			                  +" from ref_sector " 
			                 + " order by sector_nme "
			                 );		        
			appADatabaseAccess.doDbTabMetadataExcelSheet(aFileExcelPOI,"ref_sector");
            //	        
			appADatabaseAccess.doExcelSqlUsed(acomm, aFileExcelPOI);
	   		try {
				aFileExcelPOI.doOutputEnd(acomm);
			} catch (IOException e) {
				throw new AException(acomm, e, " Close of outFileExcel");
			}		        
            //
	    	acomm.dbConClose();
	    	acomm.end();
			//
		} catch (AExceptionSql e1) {
			acomm.addPageMsgsLineOut("MainMarketDataResults AExceptionSql msg{"+e1.getMessage()+e1.getExceptionMsg()+"}");
			throw e1;

		} catch (AException e1) {
			acomm.addPageMsgsLineOut("MainMarketDataResults AException msg{"+e1.getMessage()+"}");
			throw e1;
		
			
		}

	}	
	//

//
// END CLASS
//	
}
