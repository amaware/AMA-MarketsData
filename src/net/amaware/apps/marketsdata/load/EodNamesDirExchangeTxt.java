/**
 * 
 */
package net.amaware.apps.marketsdata.load;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.amaware.markets.proc.Enums.FileProcessStatus;
import com.amaware.markets.proc.FeedDataStoreReport;
import com.amaware.markets.query.REF_EXCHANGE;

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

public class EodNamesDirExchangeTxt extends FeedDataStoreReport {
/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	final String thisClassName = this.getClass().getName();	
	//
	//*SqlApp AutoGen @2016-04-20 21:46:04.0
    protected ADataColResult fExchCd = mapDataCol("Code");
    protected ADataColResult fExchangeNme = mapDataCol("Name");
    protected ADataColResult fModTs = mapDataCol("mod_ts");
    protected ADataColResult fModUserid = mapDataCol("mod_userid");
    //add col for msg
    protected ADataColResult fMsg = mapDataCol("Message");
	// 
	AFileO outFile = new AFileO();
	//
	String extractFileNameProperty = "extractNameFull";
	String extractFileName    = "";
	//
	int fileRowNum = 0;
	int dataRowNum = 0;
	//
	REF_EXCHANGE qREF_EXCHANGE = new REF_EXCHANGE();
	//
	/**
	 * 
	 */
	public EodNamesDirExchangeTxt(ACommDb acomm, File file) {
		//                 subject, topic, item, description
		super(acomm, file, "feed", "ref", "exchanges","markets exchange codes");
		//set file attributes
	    mainApp.setSourceHeadRowStart(0);
	    mainApp.setSourceDataHeadRowStart(1);
	    mainApp.setSourceDataHeadRowEnd(1);
	    mainApp.setSourceDataRowStart(2);

	}

	@Override
	public boolean doSourceStarted(ACommDb acomm) {
		boolean retb = super.doSourceStarted(acomm);
		if (!retb) { return retb; };
		
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
		
	   acomm.addPageMsgsLineOut(thisClassName
             +" |extractFileName=" + extractFileName			    
		     +"______________________"
			 );
		
	   
		return retb;
	}
	
	@Override
	public boolean doSourceEnded(ACommDb acomm) {
		super.doSourceEnded(acomm);
		
		outFile.closeFile();
		
		return true;
	}
	
	@Override
	public boolean doDataHead(ACommDb acomm, int _recNum
			//, Vector dataFields
			)
			throws AException {

		 super.doDataHead(acomm, _recNum);
		  //if no sql statement on report, comment out next line 
		  
		  //-------------check that file data header is correct for expected values
		  if (!compareFileDataStoreDataHead(acomm, 2)) {
				throw new AException(acomm, thisClassName 
						+ "=>File Data Head Fields do NOT Match Mapped Fields." 
	               		+ " |File Data Head{" + getSourceDataHeadList().toString()					
						);
		  }
		//
	    return true;
	    //		return false to end processing    

	} 	
	@Override
	public boolean doDataRowsNotFound(ACommDb acomm) throws AException {
		super.doDataRowsNotFound(acomm);
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
		//
		if (fileRowNum > 10) {
			acomm.addPageMsgsLineOut(thisClassName+"=>InRow#{" + fileRowNum + "}"
					+ " Test Limit reached...end for file{"+getSourceName()+"}"
					);	
			return false;
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
	 		   fModTs.setColumnValue(getTransTS());
	 		   fModUserid.setColumnValue(acomm.getDbUserID());
               //set db cols from mapped fields
	 		   doDSRFieldsToTableREF_EXCHANGE(acomm);	 		   
	 		   //insert
	    	   qREF_EXCHANGE.doProcessInsertRow(acomm);
	    	   //
	    	   setRowsInsertedOkCtr(getRowsInsertedOkCtr() + 1);
	    	   //
	       } catch (AExceptionSql e1) {
			  if (e1.isExceptionSqlRowDuplicate(acomm)) { //
				  // this.dataRowAppendLine("Not Inserted for ID{"+fExchCd.getColumnValue()+"}"
				  
				  //qREF_EXCHANGE.doProcessDeleteRowPK(acomm);
		          //qRATE_SURCHARGES.doProcessInLine(acomm
	 	          //         , "DELETE FROM rate_surcharges "
	 	          //         +   " WHERE id = "      +  fId.getColumnValue()  
	              //  		//+   "   AND pod = "      + "'" +  getPod()  + "'"
	 	          //       );
				  
				  setRowsInsertedDupsCtr(getRowsInsertedDupsCtr() + 1);
				  
				  fMsg.setColumnValue("Not Inserted for ID{"+fExchCd.getColumnValue()+"}"
						            + "...msg{"+e1.getExceptionMsg()+"}"
						             //, this.htmlLineErrorStyle);
				                     , this.htmlColErrorStyle);
			   } else {
				   throw e1;
			   }
           }
		   
		   if (qREF_EXCHANGE.getPsNumRowsInserted() > 0) {
			   
			   //this.dataRowAppendLine("...Row Inserted for {"+qREF_EXCHANGE.getInsertStatement(acomm)+"}"
			   fMsg.setColumnValue("...Row Inserted for {"+qREF_EXCHANGE.getInsertStatement(acomm)+"}"					   
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
		//int lineNum=0,colNum=0,colNumMax=10;
		//StringBuffer outLineBuff = new StringBuffer();
		//
		super.doDataRowsEnded(acomm);		
	    //
		getThisHtmlServ().outPageLine(acomm, "Data Rows Ended." 
	                                  +" #Rows Inserted{"+getRowsInsertedOkCtr()+"}"
	                                  +" #Rows NOT Inserted..dups{"+getRowsInsertedDupsCtr()+"}"
				                      ,htmlTitleStyle);
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
 	    	
 	    	//super.doDataRowsEnded(acomm);
 	    	throw new AException(acomm, "Requested MAX ROWS EXCEEDED...MORE ROWS EXIST" 
               		+ " @SourceRows#" + getSourceRowNum()
               		+ " @DataRow#" + getDataRowNum()
    				+" |#MaxRows=" + getSourceDataRowEndNum()
 	    					);
         }
		
        
		//return super.doDataRowsEnded(acomm);
        return true;
		
	}
	
	/*
	 * 
	 */

	//
	//*SqlApp AutoGen @2016-04-20 21:46:04.0
	 public void doDSRFieldsValidate(ACommDb acomm) throws Exception {
	    fExchCd.setColumnValue(doFieldValidateString(acomm, fExchCd.getColumnValue()));
	    fExchangeNme.setColumnValue(doFieldValidateString(acomm, fExchangeNme.getColumnValue()));
	    //fModTs.setColumnValue(doFieldValidateString(acomm, fModTs.getColumnValue()));
	   //fModUserid.setColumnValue(doFieldValidateString(acomm, fModUserid.getColumnValue()));
	//
	 } //End doDSRFieldsValidate
	//
	//* SqlApp DataStoreReport SET Table columns from DSR
	 //
	 //*SqlApp AutoGen @2016-04-20 21:46:04.0
	  public void doDSRFieldsToTableREF_EXCHANGE(ACommDb acomm) {
	              doDSRFieldsToTableREF_EXCHANGE(acomm,  qREF_EXCHANGE);
	 //
	  } //End doDSRFieldsToTableREF_EXCHANGE qREF_EXCHANGE
	 //
	 //
	  public void doDSRFieldsToTableREF_EXCHANGE(ACommDb acomm, REF_EXCHANGE _qClass) {
	    _qClass.setExchCd(fExchCd.getColumnValue());
	    _qClass.setExchangeNme(fExchangeNme.getColumnValue());
	    _qClass.setModTs(fModTs.getColumnValue());
	    _qClass.setModUserid(fModUserid.getColumnValue());
	 //
	  } //End doDSRFieldsToTable REF_EXCHANGE _qClass
	 //
	 //
	 //* SqlApp DataStoreReport SET Data Fields from Table columns
	 //
	 //*SqlApp AutoGen @2016-04-20 21:46:04.0
	  public void doDSRFieldsFromTableREF_EXCHANGE(ACommDb acomm) {
	              doDSRFieldsFromTableREF_EXCHANGE(acomm,  qREF_EXCHANGE);
	 //
	  } //End doDSRFieldsFromTableREF_EXCHANGE
	 //
	  public void doDSRFieldsFromTableREF_EXCHANGE(ACommDb acomm, REF_EXCHANGE _qClass) {
	     fExchCd.setColumnValue(_qClass.getExchCd());
	     fExchangeNme.setColumnValue(_qClass.getExchangeNme());
	     fModTs.setColumnValue(_qClass.getModTs());
	     fModUserid.setColumnValue(_qClass.getModUserid());
	 //
	  } //End doDSRFieldsFromTable qREF_EXCHANGE
	 //
	
	
	//
	// END
	//	
 }
