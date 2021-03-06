/**
 * 
 */
package net.amaware.apps.marketsdata.load;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.poi.ss.usermodel.Sheet;

import net.amaware.apps.marketsdata.load.MainMarketDataLoads.DirectoryName;
import net.amaware.appsbase.datatrack.DataTrackAccess;
import net.amaware.autil.AComm;
import net.amaware.autil.ACommDb;
import net.amaware.autil.ACommDbFile;
import net.amaware.autil.ADataColResult;
import net.amaware.autil.ADatabaseAccess;
import net.amaware.autil.AException;
import net.amaware.autil.AExceptionSql;
import net.amaware.autil.AFileExcelPOI;
import net.amaware.autil.AFileO;
import net.amaware.serv.HtmlTargetServ;
import net.amaware.serv.SourceProperty;


/**
 * @author PSDAA88 - Angelo M Adduci - Sep 6, 2005 3:02:12 PM
 * 
 */

public class EodDirectoryNames extends DataTrackAccess  {
/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	final String thisClassName = this.getClass().getSimpleName();	
	//
//	AFileExcelPOI aFileExcelPOI = new AFileExcelPOI(); 
//	Sheet aSheetDetail;
	/**
	 * 
	 */
	public EodDirectoryNames(ACommDb acomm, String ipropFileName) {
		//                 subject, topic, item, description
		super(acomm, ipropFileName);
		//set file attributes
		//
	}

	public boolean doProcess(ACommDbFile acomm) {
		
		doDirectoryFileProcess(acomm, Arrays.asList(".txt"));
		
		return true;
	}	
	
	public void doFileFound(ACommDbFile acomm, String _fileFullName) {
		//acomm.addPageMsgsLineOut(thisClassName+"=>Input File{" + _fileFullName +"}");
		
		acomm.addPageMsgsLineOut(" ");
		String filenamename=acomm.getFileNameName(_fileFullName);
		
	    try {		
	    	switch (filenamename.toLowerCase()) {

             case "exchanges.txt": 
	        	   acomm.addPageMsgsLineOut(thisClassName+" Processing for{"+filenamename+"}");
	        	   doFileTracking(acomm, _fileFullName
	        				     , "eodfeed", "ref", "exchanges", "Markets Exchange Codes"
	        				     );        	   
	        	   
	        	   EodDirectoryNamesExchanges aEodNamesDirExchangeTxt = new EodDirectoryNamesExchanges(acomm, this);
	        	   
	        	   //aEodNamesDirExchangeTxt.doProcess(acomm, thisClassName);
	        	   
	        	   doFileProcessed(acomm,_fileFullName);
	        	  
	   	           //
	               break;
	    	
	    	case "amex.txt":
	    	case "forex.txt":
	    	case "nasdaq.txt":
	    	case "nyse.txt":
	    	case "usmf.txt":
				   acomm.addPageMsgsLineOut(thisClassName+"Processing for{"+filenamename+"}");
	        	   doFileTracking(acomm, _fileFullName
      				     , "eodfeed", "ref", "exchanges-symbols", "Symbols for each Exhanges Codes"
      				     );        	   
      	   
	        	   EodDirectoryNamesExchangesSymbols aEodDirectoryNamesExchangesSymbols = new EodDirectoryNamesExchangesSymbols(acomm, this);
      	   
      	           //aEodNamesDirExchangeTxt.doProcess(acomm, thisClassName);
      	   
      	           doFileProcessed(acomm,_fileFullName);
	   	       
	               break;

	           
	        default: 
		    	  //processFile(acomm, inFile);
		    	   String outmsg=thisClassName+"===UNKNOWN Process for File  Name{"+filenamename+"}";
				   acomm.addPageMsgsLineOut(outmsg);
		    	  //throw new AException(acomm, outmsg);	    	
	  	    }
    	
  	
	    	
        } catch (AExceptionSql e1) {
            throw e1;
        }		    	
		
	}
	
	public void doFilesEnded(ACommDbFile acomm, int _filesFound) {
		//acomm.addPageMsgsLineOut(thisClassName+"=>No Method Override for Input File{" + _fileFullName +"}");
	}	
	//
	// END
	//	
 }
