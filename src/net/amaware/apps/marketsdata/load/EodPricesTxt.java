/**
 * 
 */
package net.amaware.apps.marketsdata.load;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Vector;


import net.amaware.app.DataStoreReport;
import net.amaware.autil.ACommDb;
import net.amaware.autil.ADataColResult;
import net.amaware.autil.AException;
import net.amaware.autil.AFileO;
import net.amaware.serv.HtmlTargetServ;
import net.amaware.serv.SourceProperty;


/**
 * @author PSDAA88 - Angelo M Adduci - Sep 6, 2005 3:02:12 PM
 * 
 */

public class EodPricesTxt extends DataStoreReport {
/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	final String thisClassName = this.getClass().getName();	
	final String htmlLineBreakStyle = "color:orange;background-color:white;font-size:1em;border:0em;border-bottom:1em;padding:.5em;";	
	final String htmlLineErrorStyle = "color:white;background-color:maroon;font-size:.7em";
	final String htmlLineOkStyle = "color:green;background-color:white;font-size:.9em;border:0em;padding:.5em;";
	final String htmlColErrorStyle = "color:white;background-color:maroon;font-size:.7em";
	final String htmlColOkStyle = "color:green;background-color:white;font-size:.7em";
	final String htmlTitleStyle="font-weight:bold;border:solid white 1em;background-color:gray;color:white;padding:.3em;";
	final String htmlHeadingStyle="color:olive;";
	final String htmlTrailerStyle="color:orange;border:0em;margin-bottom:2em;";	
	//
	public enum FileRecType {  AMEX, NYSE, NASDAQ, USMF, MLSE,NONE; };
	FileRecType fileRecType;
    //	
	public enum RecSfoStatus {  NotFound, Reconciled, RECON, ORPHAN;}; 
	RecSfoStatus sfoStatus = RecSfoStatus.NotFound;
    //	
	//
	//# ticker date open high low close vol 
/**/	
	ADataColResult fTicker = mapDataCol("ticker");
	ADataColResult fDate = mapDataCol("date");
	ADataColResult fOpen = mapDataCol("open");
	ADataColResult fHigh = mapDataCol("high");
	ADataColResult fLow = mapDataCol("low");
	ADataColResult fClose = mapDataCol("close");
	ADataColResult fVol = mapDataCol("vol");
/**/		
	//ADataColResult DISPLAY_OUT = mapDataCol();
	//
	String sMdnNPA="";
	String sMdnNXX="";
	String sMdnTLN="";
	String sCustIdNoPrev="";
	int numCustRows=0;
	//
	String outFileNamePrefix="";
	String fileRecTypePrevName;	
	//
	//SqlPsProc sqlModify = new SqlPsProc();
	//
	StringBuffer outStringBuff  = new StringBuffer();
	char outStringDelim = '\t';
	//
	AFileO outFile = new AFileO();
	//
	String extractFileNameProperty = "extractNameFull";
	String extractFileName    = "";
	
	//
	int fileRowNum = 0;
	int dataRowNum = 0;
	int acctNumCnt = 0;
	int acctNumDisplayCnt       = 0;
	int acctNumDisplayMaxCnt    = 100;
	int mtnRowsFound = 0;	
	//
	private java.sql.Timestamp currTimestamp = new java.sql.Timestamp(
			(new java.util.Date()).getTime());

	private java.sql.Timestamp aTimestamp = new java.sql.Timestamp(
			(new java.util.Date()).getTime());
	//
    //	
	/**
	 * 
	 */
	public EodPricesTxt() {
		super();

	}

	public DataStoreReport processThis(ACommDb acomm, SourceProperty _aProperty,
			HtmlTargetServ _aHtmlServ) {
		
		super.processThis(acomm, _aProperty, _aHtmlServ); // always call this
		outFileNamePrefix= getThisHtmlServ().getDirFileName().replaceAll(".txt.html", "");

		
		//_aProperty.displayProperties(acomm);
		
		//_aProperty.setValue(SourceProperty.getPropDataRowEnd(), 15);
		//
		acomm.addPageMsgsLineOut(thisClassName
		  + ":processThis StatementId=" + getAStatementsID()
          + " |SourcePropertyFileName=" + _aProperty.getNameFull(acomm)
		 );

		acomm.addPageMsgsLineOut(
				    "        "
				  + " |dbMaxRowsToReturn=" + acomm.getDbRowsMaxReturn()
				  + " |PropertyNumberOfMaxDataRows=" + _aProperty.getValue(SourceProperty.getPropDataRowEnd())
				 );
		

		
		return this;
	}
	

	public boolean doSourceStarted(ACommDb acomm) {
		int _currRowNum = getDataRowNum();		
		boolean retb = true;
		
		acomm.addPageMsgsLineOut(" ");
		
		try {
			extractFileName = acomm.getPropertyValue(extractFileNameProperty);
			
			if (extractFileName == null || extractFileName.length() == 0) {
				//throw new AException(acomm, "extractFile Open Needs Property File value for=>" 
				//		+ extractFileNameProperty);
			}
			
			outFile.openFile(extractFileName);
		} catch (IOException e1) {
	
			throw new AException(acomm, e1, "extractFile Open=>");
		}
		
	   String inFileName = acomm.getPropertyValue("inputFileNameName");	
	   acomm.addPageMsgsLineOut(thisClassName
             +" |inputFileName=" + inFileName
             +" |extractFileName=" + extractFileName			    
		     +"______________________"
			 );
		
	   if (inFileName.isEmpty()) {
		   throw new AException(acomm, "input File Name is empty");
	   } else {
		   if (inFileName.toLowerCase().startsWith("nyse_")) {
			   fileRecType = FileRecType.NYSE;
		   } else if (inFileName.toLowerCase().startsWith("nsqa")) {
			   fileRecType = FileRecType.NASDAQ;
		   } else if (inFileName.toLowerCase().startsWith("usmf")) {
			   fileRecType = FileRecType.USMF;
		   } else {
			   throw new AException(acomm, "Invalid Input File prefix...unknown{"+inFileName+"}");
		   }
	   }
	   
		return retb;
	}
	
	public boolean doSourceEnded(ACommDb acomm) {
		super.doSourceEnded(acomm);
		
		outFile.closeFile();
		
		return true;
	}
	
	public boolean doSourceCmdLine(ACommDb acomm, int recNum, ArrayList<String> _arrString) {
		//super will display _arrStringLines that exist before first data row
		//super.doSourceCmdLine(acomm, recNum, _arrString); 
			  return true;
	}	
	

	public boolean doDataHead(ACommDb acomm, int _recNum
			//, Vector dataFields
			)
			throws AException {
		int colNum = 0;

		  //if no sql statement on report, comment out next line 
         //setUserTitle2(getThisHtmlServ().formatForSqlout(acomm, getThisStatement()));
		
		 setUserTitle("Exchange {"+fileRecType.toString()+"}" 
				 + " File {"+acomm.getPropertyValue("inputFileNameName")+"}"
                 );
         setUserTitle2(thisClassName);
        
         StringBuffer _strBuff = new StringBuffer();

         _strBuff.setLength(0);
         Enumeration en = getDataColNames().elements();
	     while (en.hasMoreElements()) {
	    	 _strBuff.append("|" + ((String) en.nextElement()));
		  }
	     _strBuff.append("|");
		  acomm.addPageMsgsLineOut(thisClassName
					+ "=>DataColNames=" + _strBuff.toString());
		  
         _strBuff.setLength(0);
         en = getDataColTitles().elements();
	     while (en.hasMoreElements()) {
	    	 _strBuff.append("|" + ((String) en.nextElement()));
		  }
	     _strBuff.append("|");
		  acomm.addPageMsgsLineOut(thisClassName
					+ "=>DataColTitles=" + _strBuff.toString());

		  
		super.doDataHead(acomm, _recNum);		

		
		//
        
	    return true;
	    //		return false to end processing    

	}
	
	public boolean doDataRowsNotFound(ACommDb acomm) throws AException {

		 //if no sql statement on report for not found, comment out next line		
		setUserTitle2(getThisHtmlServ().formatForSqlout(acomm, getThisStatement()));
		super.doDataRowsNotFound(acomm);
		
		//throw new AException(acomm, "DataRowsNot Found");
		return true;

	}

	/*
	 * 
	 * 
	 */

	public boolean doDataRow(ACommDb acomm, AException _exceptionSql, boolean _isRowBreak)
			throws AException {
		//If controlling row processing, then put in the next statement 
		// and then to put out this row...super.doDataRow()
		// ....else super will put out row		
		if (!_exceptionSql.isExceptionNone()) {
			throw _exceptionSql;
		}
		//
		++fileRowNum;
		//
		//if (APP_COL_2.getColumnValue().compareTo("2") == 0) {
    	//	APP_COL_1.setColumnValue(APP_COL_1.getThisClassName());
		//}
		
		//APP_COL_1.setColumnValue(APP_COL_1.getThisClassName());

		//int _currRowNum = getSourceRowNum();
		if (fileRowNum > getSourceDataRowEndNum()) {
			throw new AException(acomm, thisClassName 
					+ "=>More rows retuned than expected. CurrentRow#=" + fileRowNum
               		+ " |@DataRow#" + getDataRowNum()					
               		+ " |@SourceRow#" + getSourceRowNum()
    				+ " |#MaxRows=" + getSourceDataRowEndNum()
					
					);
			
		}
		//
		switch (fileRowNum) {
		case 1:
			//getThisHtmlServ().outPageLine(acomm, fRowNum.getColumnValue(),"color:black;background-color:white;");
			break;
		case 2:			
			
			break;

		default:
			//dataRowThis(acomm, _exceptionSql, _isRowBreak);
		}
		/*
		acomm.addPageMsgsLineOut("=>" + thisClassName
						+ "=>fField1{" + fFieldOne.getColumnValue() + "}"
						+ " |fField2{" + fFieldTwo.getColumnValue()  + "}"
						+ " |fField3{" + fFieldThree.getColumnValue()  + "}"
						+ " |fField4{" + fFieldFour.getColumnValue()  + "}"
						);
		*/		
				
		if (!isDataRowOut()) {
			   super.doDataRow(acomm, _exceptionSql, _isRowBreak);
		}

		//getThisHtmlServ().outPageLine(acomm, fCobolPrefix.getColumnValue(),"color:black;background-color:white;");
		//getThisHtmlServ().outPageLine(acomm, getDataRowColsToString(),"color:black;background-color:white;");
		/**/
		//acomm.addPageMsgsLineOut("=>" + thisClassName
		/*
		getThisHtmlServ().outPageLine(acomm, 
				  "fField1{" + fFieldOne.getColumnValue() + "}"
				+ " |fField2{" + fFieldTwo.getColumnValue()  + "}"
				+ " |fField3{" + fFieldThree.getColumnValue()  + "}"
				+ " |fField4{" + fFieldFour.getColumnValue()  + "}"
				,"color:black;background-color:linen;padding:.1em;"
				);
        */				
		/**/
		
		
		
		return true; // or false to stop processing of file

	}
	
	
	
	/* (non-Javadoc)
	 * @see net.amaware.app.DataStoreReport#doDataRowsEnded(net.amaware.autil.ACommDb)
	 */
	public boolean doDataRowsEnded(ACommDb acomm) throws AException {
        //
		int lineNum=0,colNum=0,colNumMax=10;
		StringBuffer outLineBuff = new StringBuffer();
		//
	    //
		getThisHtmlServ().outPageLine(acomm, "Source File Ended",htmlTitleStyle);
		//
		//
		acomm.addPageMsgsLineOut(thisClassName
				    +":doDataRowsEnded=>StatementId=" + getAStatementsID() 
               		+ " @SourceRows#" + getSourceRowNum()
               		+ " @DataRow#" + getDataRowNum()
    				+ " |#MaxRows=" + getSourceDataRowEndNum()
    				);
		
        if (getSourceRowNum() >  getSourceDataRowEndNum()) {
 	    	getThisHtmlServ().outPageLineWarning(acomm,  
	    			 "More data from Source may exist....ended due to requested max rows"
               		+ " |SourceRows#" + getSourceRowNum()
               		+ " |DataRow#" + getDataRowNum()
    				+" |#MaxRows=" + getSourceDataRowEndNum()
 	    			
					);
 	    	
 	    	super.doDataRowsEnded(acomm);
 	    	throw new AException(acomm, "Requested MAX ROWS EXCEEDED...MORE ROWS EXIST" 
               		+ " @SourceRows#" + getSourceRowNum()
               		+ " @DataRow#" + getDataRowNum()
    				+" |#MaxRows=" + getSourceDataRowEndNum()
 	    					);
         }
		
        
		return super.doDataRowsEnded(acomm);
		
	}
	
	/*
	 * 
	 * 
	 */



	
	
	//
	// END
	//	
 }
