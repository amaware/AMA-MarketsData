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

import net.amaware.autil.*;
//
/**
 * @author AMAWARE - Angelo M Adduci
 * 
 */
public class MainMarketDataLoads {
	final static String thisClassName = "MainMarketDataLoads";	
	// set Properties file to being used
    final static String propFileName            = "MainMarketDataLoads.properties";

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
	static HtmlTargetServ aHtmlServReport;
    static AFileExcelPOI aFileExcelPOI = new AFileExcelPOI(); 
	Sheet aSheetDetail;
	Sheet aSheetResult;
	Sheet aSheetMetaData;
	Sheet aSheetLog;
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
		
		try { //setup the com class with properties file and log file prop key
			acomm = new ACommDbFile(propFileName, args);
			
			String dirLastName="";
			
			acomm.addPageMsgsLineOut(thisClassName+"=>getArgFilePath{" + acomm.getArgFilePath() +"}"
					                              +" |ArgFilePathDirName{" + acomm.getArgFilePathDirName() +"}"
					                              );			
			
			if (AComm.isArgFileDirectory()) {
			
				acomm.getArgFilePath();
				
				
			}
			
  		    //for (String thisFileName: acomm.getFileList(Arrays.asList(".xls",".xlst",".txt"))) {
			for (String thisFileName: acomm.getFileList(Arrays.asList("*.*"))) {
  		    	   acomm.addPageMsgsLineOut(thisClassName+"=>Input File{" + thisFileName +"}");
  		    	   //
					int colHeadRow=2;
					int colDetRow=3;
					//
   
  		    	   
  		    	   //
					
					acomm.addPageMsgsLineOut(thisClassName+"==>File ColHeaderRow{" + colHeadRow +"}"
							                +" |ColDetailRowStart{" + colDetRow +"}");
					/*
  		    	    ExcelDbTable aExcelDbTable = new ExcelDbTable();
  		    	    
					mainApp = new MainAppDataStore(acomm, aExcelDbTable, thisFileName, acomm.getFileTextDelimTab());
  		    	    //mainApp = new MainAppDataStore(acomm, new ExcelDbTable(), thisFileName, '|');
					//
					mainApp.setSourceHeadRowStart(1);

					//
					mainApp.setSourceDataHeadRowStart(colHeadRow);
					mainApp.setSourceDataRowStart(colDetRow);
					//
					//mainApp.setSourceDataRowEnd(10);
					
					aExcelDbTable.setMainAppDataStore(mainApp);
					
					//
					mainApp.doProcess(acomm, "MainExcelDbTable");
					*/		
  		    }
  		    
			//mainApp = new MainAppDataStore(acomm, _fileProcess, args, acomm.getFileTextEndLine(), 3);
			//mainApp = new MainAppDataStore(acomm, _fileProcess, args, ' ');
			//mainApp = new MainAppDataStore(acomm, _fileProcess, args, acomm.getFileTextDelimTab());
			
			//mainApp.getHtmlServ().outPageLine(acomm, thisClassName+" completed ");
			acomm.end();
			
		} catch (AException e1) {
			acomm.addPageMsgsLineOut("MainExcelDbLoad AException msg{"+e1.getMessage()+"}");
			throw e1;
		}

	}	
	//
	public static void mainold(String[] args) {
		//
		String dirName="";
		String dirPath="";
		try {  
			acomm = new ACommDb(propFileName);
		    //String inFileName 
		    File inFile = inputArgsSql(acomm,args);
		    
	    	aHtmlServReport = new HtmlTargetServ(acomm
				    , inFile.getPath() //inFile.getPath() 
				      + ".html"
				    , thisClassName + " for " + acomm.getUserName()
				    //, getReportStyleRow()
				    //, getReportStyleRowCol()
				    
				    //, optList
				    );		    	
		    
	    	//aHtmlServReport.outTargetLine(acomm, thisClassName,"");
	    	//aHtmlServReport.echo("<table>");	    	
	    	
	    	Vector<String> aVector = new Vector<String>();
	    	aVector.addElement( new String (thisClassName));
	    	
		    if (inFile.isDirectory()) {
		    	dirPath=inFile.getPath();
		    	dirName=inFile.getName();
		    	
		    	//DirectoryName =dirName;
		    	
		    	aVector.addElement(dirPath);		    	
		    	aHtmlServReport.outTargetHeading(acomm, aVector);

		    	switch (dirName.toLowerCase()) {
	               case "fundamentals": 
	            	    directoryName = DirectoryName.FUNDAMENTALS;
	                    break;

	               case "names": 
	            	    directoryName = DirectoryName.NAMES;
	                    break;
	                    
	               default: 
	   		    	//processFile(acomm, inFile);
	   		    	String outmsg="===UNKNOWN Directory Name===>"+inFile.getName();
	   		    	
	   		    	aHtmlServReport.outTargetLine(acomm, thisClassName+"=>"+outmsg);
	   		    	
	   		    	throw new AException(acomm, outmsg);	    	
	                    
		    	}
               				
		    	 File[] listOfFiles = inFile.listFiles();
		    	 for (File file : listOfFiles) {
		    	    if (file.isFile()) {
		    	    	if (file.getName().endsWith(".txt")){
		    	    	    if (file.getName().endsWith("readme.txt")) {
		    	    	    	aHtmlServReport.outTargetLine(acomm, "File bypassed...=>"+file.getName(),"color:orange;border:solid orange .1em;");
		    	    	    	
		    	    	    } else if (file.getName().endsWith("report.txt")) {		    	    	    	
		    	    	    	aHtmlServReport.outTargetLine(acomm, "File bypassed...=>"+file.getName(),"color:orange;border:solid orange .1em;");
		    	    	    	
		    	    	    } else {
		    	    	    	 try {	    	    	
                                 	switch (directoryName) {
									  case FUNDAMENTALS:
										/*   
										   thisDataStore = eodFundamentalsDirFile = new EodFundamentalsDirFile();
										   
			    	    	    	       mainApp = new MainAppDataStore(acomm, thisDataStore, file.getPath(), acomm.getFileTextDelimTab());
			    	    	    	       mainApp.setSourceHeadRowStart(0);
			    					       mainApp.setSourceDataHeadRowStart(1);
			    					       mainApp.setSourceDataHeadRowEnd(1);
			    					       mainApp.setSourceDataRowStart(2);
			    					       mainApp.doProcess(acomm, thisClassName);
			    					       
			    					       if (eodFundamentalsDirFile.fileProcessStatus == FileProcessStatus.Processed) {
			    					    	   aHtmlServReport.outTargetLine(acomm, "Completed{"+file.getPath()+"}","color:green;");
			    					       } else {
			    					           aHtmlServReport.outTargetLine(acomm, "Not Processed Status returned for file{"+file.getPath()+"}"
			    					        		   + ".....Status{" + eodFundamentalsDirFile.fileProcessStatus +  "}"
			    					        		   ,"color:red;font-weight:bold;");
			    					       }
										   
										  */ 
										   break;
											
									  case NAMES:
	                                       aHtmlServReport.outTargetLine(acomm, "Processing{"+file.getPath()+"}" 
									                                     //+ " Name{"+file.getName()+"}"
	                                    		                         ,"color:blue;border:solid blue .2em;");
	                                       
	                                       fileNameToProcess="exchanges.txt";
										   if (file.getName().toLowerCase().contentEquals(fileNameToProcess)){
											   try {
												   
											       thisDataStore = new EodNamesDirExchangeTxt(acomm, file);
											       
											       //thisDataStore.setFileProcessStatus(FileProcessStatus.Reload);
											       
				    					           thisDataStore.doProcess(acomm, thisClassName, aHtmlServReport);

				    					           if (thisDataStore.getFileProcessStatus() == FileProcessStatus.Processed) {
				    					           } else {
				    					           }
				    					           
											   } catch (AException e) {
				    					    	   //aHtmlServReport.outTargetLine(acomm, "EXCEPTION{"+e.getMessage()+"}"  
					    			               //          ,"border-bottom:solid red .2em;color:maroon;font-weight:bold;");
												 throw e;
											   } 
											   
										   } else {
		                                 		aHtmlServReport.outTargetLine(acomm, "Bypassed Unknown File Name {"+file.getName()+"}"+ " Expecting{"+fileNameToProcess+"}","color:orange;");
										   }

										   break;
										   
										default:
											 break;
										}
		    	    	    		 
		    	   			  } catch (AException e) {
		    	   				  
					    	         aHtmlServReport.outTargetLine(acomm
   					    	        		 , "AEXCEPTION Msg{"+e.getMessage()+"}" 
  			                                 ,"border-top:solid red .2em;color:maroon;font-weight:bold;");
   					    	         
   					    	         aHtmlServReport.outTargetLine(acomm
   					    	        		 , " ExpMsg{"+e.getExceptionMsg()+"}" 
  			                                 ,"border-bottom:solid red 0em;color:maroon;font-weight:bold;");
   					    	         
   					    	         aHtmlServReport.outTargetLine(acomm
   					    	        		 , " Code{"+e.getExceptionCode()+"}" + " State{"+e.getExceptionState()+"}"  
  			                                 ,"border-bottom:solid red .2em;color:maroon;font-weight:bold;");
   					    	         
		    					     throw e;
		    					  
		    	   			  } catch (Exception e) {
				    	         aHtmlServReport.outTargetLine(acomm, "EXCEPTION{"+e.getMessage()+"}"  
		  			                         ,"border-bottom:solid red .2em;color:maroon;font-weight:bold;");
		    	   				  
				    	           throw e;
		    					  //throw new AException(acomm,"Msg{"+e.getMessage());
		    					  
		    				  }

		    	    	    }
			    	        
		    	    	} else {		    	    		
		    	    		aHtmlServReport.outTargetLine(acomm, "File bypassed...not .txt=>"+file.getPath(),"color:orange;border:solid orange .1em;");	
		    	    	}
		    	    }
		    	}
		    	
		    	
		    } else {
		    	aHtmlServReport.outTargetHeading(acomm, aVector);
		    	
		    	//processFile(acomm, inFile);
		    	String outmsg="===NOT a Directory Name===>"+inFile.getName();
		    	
		    	aHtmlServReport.outTargetLine(acomm, thisClassName+"=>"+outmsg);
		    	throw new AException(acomm, outmsg);	    	
		    }

		    aHtmlServReport.outTargetLine(acomm, thisClassName+" completed ","text-align:center;color:purple;");
			//mainApp.getHtmlServ().outPageLine(acomm, thisClassName+" completed ");
		    //
		    aHtmlServReport.outTargetTrailer(acomm, aHtmlServReport.getDirFileName());
		    
		    //aHtmlServReport.echo("</body>");
		    
			acomm.end();
			
		} catch (AException e1) {
			throw e1;
		}

	}

	
	   public static File inputArgsSql(ACommDb acomm, String[] args) {
			
			/**/
			String currentDirectory;
			File file = new File(".");
			currentDirectory = file.getAbsolutePath();
			System.out.println("Initial working directory : "+currentDirectory);
			/**/		   
		   
		    System.out.println("====Args Input Process==========");
		    String filename="";
		    if (args.length == 0) {        
		    	System.out.println("========None==========");
		    	
			    	   throw new AException(acomm
				    		, "No input arguments...FileName to process is required. Must beging with tablename");
		    	
		    } else {
		    	System.out.println("--Args Input---len=" + args.length);	    	
		    	for (int i = 0; i < args.length; i++) {
			    	System.out.println(i + ">" + args[i]);    			
		    	}
				if (args[0].length() == 0) {
				    	throw new AException(acomm
					    		, "===Arg FileName needed======");	    	
				}
				
				filename=args[0];

			}
			acomm.addPageMsgsLineOut("_______________Input Args_________________");
			for (int i=0; i < args.length;i++){
		    	acomm.addPageMsgsLineOut("Arg#"+i+"=" + args[i]);
			}
			//
			File f = new File(filename);
			filename=f.getName();			
			//
			file = new File(".");
			currentDirectory = file.getAbsolutePath();
			System.out.println("Current working directory : "+currentDirectory);
			//
			return f;
			
		}
//
// END CLASS
//	
}
