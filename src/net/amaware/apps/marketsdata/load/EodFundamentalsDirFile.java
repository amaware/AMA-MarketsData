/**
 * 
 */
package net.amaware.apps.marketsdata.load;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Vector;

import com.amaware.markets.proc.Enums.FileProcessStatus;
import com.amaware.markets.query.REF_EXCH_SYMB;
import com.amaware.markets.query.REF_SECTOR;

import net.amaware.app.DataStoreReport;
import net.amaware.autil.ACommDb;
import net.amaware.autil.ADataColResult;
import net.amaware.autil.AException;
import net.amaware.autil.AExceptionSql;
import net.amaware.autil.AFileO;
import net.amaware.serv.HtmlTargetServ;
import net.amaware.serv.SourceProperty;


/**
 * @author PSDAA88 - Angelo M Adduci - Sep 6, 2005 3:02:12 PM
 * 
 */

public class EodFundamentalsDirFile extends DataStoreReport {
/**
 * 
 */
	private static final long serialVersionUID = 1L;
	final String thisClassName = this.getClass().getName();	
	//
	//ref_exch_symb--------------------------
    protected ADataColResult fSymbCd = mapDataCol("Symbol");
    protected ADataColResult fSymbNme = mapDataCol("Name");
    //ref_sector-----------------------------
    protected ADataColResult fSectorNme = mapDataCol("Sector");
    protected ADataColResult fIndustryNme = mapDataCol("Industry");
    //ref_exch_symb--------------------------
    protected ADataColResult fPriceEarn = mapDataCol("PE");
    protected ADataColResult fEarningsShare = mapDataCol("EPS");
    protected ADataColResult fDivYield = mapDataCol("DivYield");
    protected ADataColResult fSharesOutAmt = mapDataCol("Shares");
    protected ADataColResult fDivShare = mapDataCol("DPS");
    protected ADataColResult fPriceEarnGrowth = mapDataCol("PEG");
    protected ADataColResult fPriceSales = mapDataCol("PtS");
    protected ADataColResult fPriceBookValue = mapDataCol("PtB");
    //
    //ref_exch_symb + ref_exchange----------------------
    protected ADataColResult fExchCd = mapDataCol("exch_cd");
    //ref_exch_symb---------------
    //protected ADataColResult fId = mapDataCol("id");    
    protected ADataColResult fAssetClassId = mapDataCol("asset_class_id");
    protected ADataColResult fSectorId = mapDataCol("sector_id");    
    //
	//ref_sector
    //protected ADataColResult fId = mapDataCol("id");
    protected ADataColResult fSicCd = mapDataCol("sic_cd");
    protected ADataColResult fNaicsCd = mapDataCol("naics_cd");
    protected ADataColResult fSourceNme = mapDataCol("source_nme");
    //
    //common------------------------------------------------
    protected ADataColResult fModTs = mapDataCol("mod_ts");
    protected ADataColResult fModUserid = mapDataCol("mod_userid");
    //add col for msg
    protected ADataColResult fMsg = mapDataCol("Message");
    //
	//-----------variables---------------- 
	public FileProcessStatus fileProcessStatus = FileProcessStatus.Started;
	//
	String transTS="";
	int numRowsInserted=0;
	int fileRowNum = 0;
	int fileRowNumDispCnt = 0;
	int fileRowNumDispMaxCnt = 100;
	int dataRowNum = 0;
	//
	//--------------Files-----------
	String inputFileName = "";
	AFileO outFile = new AFileO();
	String outFileNamePrefix="";	
	//
	String extractFileNameProperty = "extractNameFull";
	String extractFileName    = "";
	//
	//
	//----------------------Database Class-------------------------
	REF_SECTOR qREF_SECTOR = new REF_SECTOR();
	//
	REF_EXCH_SYMB qREF_EXCH_SYMB = new REF_EXCH_SYMB();	
	//-------------------------------------------------------------
    //	
	/**
	 * 
	 */
	public EodFundamentalsDirFile() {
		super();

	}

	@Override
	public DataStoreReport processThis(ACommDb acomm, SourceProperty _aProperty,
			HtmlTargetServ _aHtmlServ) {
		
		inputFileName=_aProperty.getName(acomm);
		
		transTS=acomm.getCurrTimestampNew();
		
		super.processThis(acomm, _aProperty, _aHtmlServ); // always call this
		outFileNamePrefix= getThisHtmlServ().getDirFileName().replaceAll(".txt.html", "");

		
		//_aProperty.displayProperties(acomm);
		
		//_aProperty.setValue(SourceProperty.getPropDataRowEnd(), 15);
		//
		acomm.addPageMsgsLineOut(thisClassName
		  + ":processThis StatementId=" + getAStatementsID()
          + " |SourcePropertyFileNameFull=" + _aProperty.getNameFull(acomm)
		 );

		acomm.addPageMsgsLineOut(
				    "        "
				  + " |dbMaxRowsToReturn=" + acomm.getDbRowsMaxReturn()
				  + " |PropertyNumberOfMaxDataRows=" + _aProperty.getValue(SourceProperty.getPropDataRowEnd())
				 );
		

		
		return this;
	}
	

	@Override
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
	   } /*else {
		   if (inFileName.toLowerCase().startsWith("nyse_")) {
			   fileRecType = FileRecType.NYSE;
		   } else if (inFileName.toLowerCase().startsWith("nasdaq")) {
			   fileRecType = FileRecType.NASDAQ;
		   } else if (inFileName.toLowerCase().startsWith("usmf")) {
			   fileRecType = FileRecType.USMF;
		   } else {
			   throw new AException(acomm, "Invalid Input File prefix...unknown{"+inFileName+"}");
		   }
		  
	   }*/
	   
		return retb;
	}
	
	@Override
	public boolean doSourceEnded(ACommDb acomm) {
		super.doSourceEnded(acomm);
		
		fileProcessStatus = FileProcessStatus.Processed;
		
		outFile.closeFile();
		
		return true;
	}
	
	@Override
	public boolean doSourceCmdLine(ACommDb acomm, int recNum, ArrayList<String> _arrString) {
		//super will display _arrStringLines that exist before first data row
		//super.doSourceCmdLine(acomm, recNum, _arrString); 
			  return true;
	}	
	

	@Override
	public boolean doDataHead(ACommDb acomm, int _recNum
			//, Vector dataFields
			)
			throws AException {
		int colNum = 0;

		  //if no sql statement on report, comment out next line 
         //setUserTitle2(getThisHtmlServ().formatForSqlout(acomm, getThisStatement()));
		
		 //setUserTitle("Exchange {"+fileRecType.toString()+"}"
		setUserTitle(" File {"+acomm.getPropertyValue("inputFileNameName")+"}"
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
		  
		  acomm.addPageMsgsLineOut(thisClassName
					+ "=>DataColTitles=" + getDataColTitleListString(acomm, '|'));
		  
		  acomm.addPageMsgsLineOut(thisClassName
					+ "=>SourceDataHeadTitles=" + getSourceDataHeadListString(acomm, '|'));

		  
		  //-------------check that file data header is correct for expected values
		  if (!compareFileDataStoreDataHead(acomm, 2)) {
				throw new AException(acomm, thisClassName 
						+ "=>File Data Head Fields do NOT Match Mapped Fields." 
	               		+ " |File Data Head{" + getSourceDataHeadList().toString()					
						);
		  }
		 //
		 super.doDataHead(acomm, _recNum);		
		//
        
	    return true;
	    //		return false to end processing    

	}
	
	@Override
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

	@Override
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
		/*
		if (fileRowNum > 10) {
			acomm.addPageMsgsLineOut(thisClassName+"=>InRow#{" + fileRowNum + "}"
					+ " Test Limit reached...end for file{"+getSourceName()+"}"
					);	
			return false;
		}
		*/
		
		//

		++fileRowNumDispCnt;
		if (fileRowNumDispCnt > fileRowNumDispMaxCnt) {
			fileRowNumDispCnt=0;
			acomm.addPageMsgsLineOut(thisClassName+"=>InRow#{" + fileRowNum + "}"
					+ " SectorName{"+fSectorNme.getColumnValue()+"}"
					+ " IndustryName{"+fIndustryNme.getColumnValue()+"}"
					);			
		}

		//getSourceDataHeadList();
		
		try {
			   this.doDSRFieldsValidate(acomm);
		   } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		   }
		
	       try { 
	 		   //setup defaults
	    	   
	    	   fSourceNme.setColumnValue(inputFileName);
	    	   
	 		   fModTs.setColumnValue(transTS);
	 		   fModUserid.setColumnValue(acomm.getDbUserID());
               //set db cols from mapped fields
	 		   doDSRFieldsToTableREF_SECTOR(acomm);	 		   
	 		   //insert
	    	   qREF_SECTOR.doProcessInsertRow(acomm);
	    	   //
	       } catch (AExceptionSql e1) {
			  if (e1.isExceptionSqlRowDuplicate(acomm)) { //
				  // this.dataRowAppendLine("Not Inserted for ID{"+fExchCd.getColumnValue()+"}"
				  fMsg.setColumnValue("Not Inserted for SectorName{"+fSectorNme.getColumnValue()+"}"
						            + " IndustryName{"+fIndustryNme.getColumnValue()+"}"
						            + "...msg{"+e1.getExceptionMsg()+"}"
						             //, this.htmlLineErrorStyle);
				                     , this.htmlColErrorStyle);
			   } else {
				   throw e1;
			   }
           }
	       
		   numRowsInserted=qREF_SECTOR.getPsNumRowsInserted();
		   if (qREF_SECTOR.getPsNumRowsInserted() > 0) {
			   
			   //this.dataRowAppendLine("...Row Inserted for {"+qREF_SECTOR.getInsertStatement(acomm)+"}"
			   fMsg.setColumnValue("...Row Inserted for {"+qREF_SECTOR.getInsertStatement(acomm)+"}"					   
			            , this.htmlColOkStyle);
			   
		   }

		   
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
	@Override
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
	 */

	//
	//* SqlApp DataStoreReport SET Table columns from DSR
	//
	//*SqlApp AutoGen @2016-04-26 21:39:21.0
	 public void doDSRFieldsToTableREF_SECTOR(ACommDb acomm) {
	             doDSRFieldsToTableREF_SECTOR(acomm,  qREF_SECTOR);
	//
	 } //End doDSRFieldsToTableREF_SECTOR qREF_SECTOR
	//
	//
	 public void doDSRFieldsToTableREF_SECTOR(ACommDb acomm, REF_SECTOR _qClass) {
	   _qClass.setSectorNme(fSectorNme.getColumnValue());
	   _qClass.setIndustryNme(fIndustryNme.getColumnValue());
	  // _qClass.setId(fId.getColumnValue());
	   _qClass.setSicCd(fSicCd.getColumnValue());
	   _qClass.setNaicsCd(fNaicsCd.getColumnValue());
	   _qClass.setSourceNme(fSourceNme.getColumnValue());
	   _qClass.setModTs(fModTs.getColumnValue());
	   _qClass.setModUserid(fModUserid.getColumnValue());
	//
	 } //End doDSRFieldsToTable REF_SECTOR _qClass
	//
	//
	//* SqlApp DataStoreReport SET Data Fields from Table columns
	//
	//*SqlApp AutoGen @2016-04-26 21:39:21.0
	 public void doDSRFieldsFromTableREF_SECTOR(ACommDb acomm) {
	             doDSRFieldsFromTableREF_SECTOR(acomm,  qREF_SECTOR);
	//
	 } //End doDSRFieldsFromTableREF_SECTOR
	//
	 public void doDSRFieldsFromTableREF_SECTOR(ACommDb acomm, REF_SECTOR _qClass) {
	    fSectorNme.setColumnValue(_qClass.getSectorNme());
	    fIndustryNme.setColumnValue(_qClass.getIndustryNme());
	   // fId.setColumnValue(_qClass.getId());
	    fSicCd.setColumnValue(_qClass.getSicCd());
	    fNaicsCd.setColumnValue(_qClass.getNaicsCd());
	    fSourceNme.setColumnValue(_qClass.getSourceNme());
	    fModTs.setColumnValue(_qClass.getModTs());
	    fModUserid.setColumnValue(_qClass.getModUserid());
	//
	 } //End doDSRFieldsFromTable qREF_SECTOR
	//
	
	 //
	//* SqlApp DataStoreReport Validate input  Data Fields
	//
	//*SqlApp AutoGen @2016-04-26 21:39:21.0
	 public void doDSRFieldsValidate(ACommDb acomm) throws Exception {
	    fSectorNme.setColumnValue(doFieldValidateString(acomm, fSectorNme.getColumnValue()));
	    fIndustryNme.setColumnValue(doFieldValidateString(acomm, fIndustryNme.getColumnValue()));
	   // fId.setColumnValue(String.valueOf(doFieldValidateInt(acomm, fId.getColumnValue(),0)));
	    fSicCd.setColumnValue(doFieldValidateString(acomm, fSicCd.getColumnValue()));
	    fNaicsCd.setColumnValue(doFieldValidateString(acomm, fNaicsCd.getColumnValue()));
	    fSourceNme.setColumnValue(doFieldValidateString(acomm, fSourceNme.getColumnValue()));
	    fModTs.setColumnValue(doFieldValidateString(acomm, fModTs.getColumnValue()));
	    fModUserid.setColumnValue(doFieldValidateString(acomm, fModUserid.getColumnValue()));
	//
	 } //End doDSRFieldsValidate
	//	
	//
	// END
	//	
 }

