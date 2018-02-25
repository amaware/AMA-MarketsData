/**
 * 
 */
package net.amaware.apps.marketsdata.load;

import java.awt.List;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Vector;

import org.apache.poi.ss.usermodel.Sheet;

import net.amaware.app.DataStoreReport;
import net.amaware.appsbase.datatrack.DataTrackAccess;
import net.amaware.appsbase.datatrack.DataTrackStore;
import net.amaware.autil.AComm;
import net.amaware.autil.ACommDb;
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

public class EodDirectoryFundamentalsExchanges extends DataTrackStore {
/**
 * 
 */
	private static final long serialVersionUID = 1L;
	final String thisClassName = this.getClass().getSimpleName();	
	//
	//ref_exch_symb--------------------------
    protected ADataColResult fSymbCd = mapDataCol("symb_cd");
    protected ADataColResult fSymbNme = mapDataCol("symb_nme");
    //ref_sector-----------------------------
    protected ADataColResult fSectorNme = mapDataCol("sector_nme");
    protected ADataColResult fIndustryNme = mapDataCol("industry_nme");
    //?--------------------------
    protected ADataColResult fPriceEarn = mapDataCol("price_earning");
    protected ADataColResult fEarningsShare = mapDataCol("earnings_share");
    protected ADataColResult fDivYield =  mapDataCol("div_yield");
    protected ADataColResult fSharesOutAmt = mapDataCol("shares_out_amt");
    protected ADataColResult fDivShare = mapDataCol("div_share");
    protected ADataColResult fPriceEarnGrowth = mapDataCol("price_earn_growth");
    protected ADataColResult fPriceSales = mapDataCol("price_sales");
    protected ADataColResult fPriceBookValue = mapDataCol("price_book_value");
    //
    //common------------------------------------------------
    protected ADataColResult fModTs = mapDataCol("mod_ts");
    protected ADataColResult fModUserid = mapDataCol("mod_userid");
    //Add fields not in input file
    protected ADataColResult fEntryDate = mapDataCol("entry_date");
    protected ADataColResult fSourceNme = mapDataCol("source_nme");    
    protected ADataColResult fExchCd = mapDataCol("exch_cd");
    protected ADataColResult fSymFundamentalId = mapDataCol("ref_exch_symb_id");
    //add col for msg
    protected ADataColResult fMsg = mapDataCol("Message");
    //
    //
	//-----------variables---------------- 
    //ADatabaseAccess thisADatabaseAccess; // in super
	static AFileExcelPOI aFileExcelPOI = new AFileExcelPOI(); 
	Sheet aSheetRequest;	 
	Sheet aSheetResult;    
	//
	String thisEntryDate="";
	String thisExchCdValue="";
	String thisSymbCdValue="";
	String thisSourceNmeValue="";
	String thisRefExchSymbId="";
	String thisRefSectorId="";
	//
	String inputFileName = "";
	String outFileNamePrefix="";	
	AFileO outFile = new AFileO();
	//
	String extractFileNameProperty = "extractNameFull";
	String extractFileName    = "";
	//	
	//
	String transTS="";
	int numRowsInserted=0;
	int fileRowNum = 0;
	int fileRowNumDispCnt = 0;
	int fileRowNumDispMaxCnt = 100;
	int dataRowNum = 0;
	//
	//--------------Files-----------
	//
	ADatabaseAccess refSectorADatabaseAccess;	
	ADatabaseAccess refExchSymbADatabaseAccess;	
    //	
	/**
	 * 
	 */

	public EodDirectoryFundamentalsExchanges(ACommDb acomm, DataTrackAccess _dataTrackAccess) {
        //
		super(acomm, _dataTrackAccess);
		//
		//Main table for input file fields gets logged
		appADatabaseAccess = new ADatabaseAccess(acomm, _dataTrackAccess.dbPropertyFileFullName
                , "sym_fundamental", true, 250); //use this number to resolve timeout of update data_track
		
		//build using Sector/Industry fields
		refSectorADatabaseAccess = new ADatabaseAccess(acomm, _dataTrackAccess.dbPropertyFileFullName
				                                , "ref_sector", true, 1000);
        //
		refExchSymbADatabaseAccess = new ADatabaseAccess(acomm, _dataTrackAccess.dbPropertyFileFullName
                , "ref_exch_symb", true, 1000);
		
		//set file attributes
		setDataTrackFileFieldDelimChar(acomm.getFileTextDelimTab());
		//
	    mainApp.setSourceHeadRowStart(0);
	    mainApp.setSourceDataHeadRowStart(1);
	    mainApp.setSourceDataHeadRowEnd(1);
	    mainApp.setSourceDataRowStart(2);
        //
	    //mainApp.setupHtmlServ();
	    //
	    mainApp.doProcess(acomm, thisClassName);
	    //
	}	
	

	@Override
	public boolean doSourceStarted(ACommDb acomm) {
		boolean retb = super.doSourceStarted(acomm);
		if (!retb) { return retb; };
		
		thisSourceNmeValue=thisDataTrackAccess.getTrackFileName();
		
		thisExchCdValue=thisDataTrackAccess.getTrackFileName().toUpperCase().replace(".TXT", "");

		//Date date = new Date();
		//thisEntryDate= new SimpleDateFormat("yyyy-MM-dd").format(date);
		thisEntryDate= thisDataTrackAccess.getTrackFileLastModifiedDate();//.substring(0,10); //"yyyy-MM-dd HH:mm:ss"
		
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
		
	    String outExcelFileName=AComm.getOutFileDirectory()+AComm.getFileDirSeperator()+thisClassName+AComm.getAppClassFileSep()
        +AComm.getArgFileName()+".report.xls";

         acomm.addPageMsgsLineOut(thisClassName+ "=>Output Excel File Name{" +outExcelFileName +"}");
		//
		aFileExcelPOI = new AFileExcelPOI(acomm, outExcelFileName);
		//
		this.getDataRowColsValueList();
		//
		aSheetRequest=aFileExcelPOI.doCreateNewSheet(thisClassName, 2
				    , Arrays.asList(".",".", acomm.getCurrTimestampAny(),thisDataTrackAccess.getTrackFileNameFull() )
				    //, Arrays.asList(fExchCd.getColumnName()
				  	 //       , fSymbCd.getColumnName()
				      //      )
				    ,  getDataRowColsNameList()        
				    
             );   		
		//
		aSheetResult=aFileExcelPOI.doCreateNewSheet("Results", 2
			    , Arrays.asList(appADatabaseAccess.getThisTableName(),appADatabaseAccess.getThisAcomm().getDbUrlDbAndSchemaName())
			    , getDataRowColsNameList()
         );   		
		
		//
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
		 //Columns are being added to map to hold DB columns 
		 /*
		  if (!compareFileDataStoreDataHead(acomm, 2)) {
				throw new AException(acomm, thisClassName 
						+ "=>File Data Head Fields do NOT Match Mapped Fields." 
	               		+ " |File Data Head{" + getSourceDataHeadList().toString()					
						);
		  }
		*/
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
		
		int fileRestartNum=0;
		
		fileRestartNum=27367;
		if (fileRowNum < fileRestartNum) {
			return true;
		} else if (fileRowNum == fileRestartNum) {
			doAppCommitControlRestart(acomm, fileRowNum);
		}
		//
		if (fSectorNme==null || fSectorNme.getColumnValue()==null || fSectorNme.getColumnValue().isEmpty()) {
			fSectorNme.setColumnValue("");
		}
		if (fIndustryNme==null || fIndustryNme.getColumnValue()==null || fIndustryNme.getColumnValue().isEmpty()) {
			fIndustryNme.setColumnValue("");
		}
		thisSymbCdValue=fSymbCd.getColumnValue();
		//
		aFileExcelPOI.doOutputRowNext(acomm, aSheetRequest
		         , getDataRowColsValueList()
			   );	 		   
		
		if (fileRowNum == 1) {
			acomm.addPageMsgsLineOut(thisClassName+"=>Mapped Cols=>" + getDataColResultListAsString());	
		}
		
	   //setup defaults
	   fEntryDate.setColumnValue(thisEntryDate);
	   fSourceNme.setColumnValue(thisSourceNmeValue);
	   fExchCd.setColumnValue(thisExchCdValue);
		   
	   fModTs.setColumnValue(getTransTS());
	   fModUserid.setColumnValue(acomm.getDbUserID());
		   
	   //acomm.addPageMsgsLineOut("Row#{"+fileRowNum+"}"+"=>DataColResultList{"+getDataColResultListAsString()+"}");
       //	
	   StringBuffer sbmsg = new StringBuffer();
	   //
       try {    
    	   refExchSymbADatabaseAccess.doQuery("select id "
                   + " FROM ref_exch_symb "
                   + " WHERE exch_cd = " +"'"+ thisExchCdValue +"'"
                   + "   AND symb_cd = " +"'"+ thisSymbCdValue +"'"
	                     );            
    	   thisRefExchSymbId="";
    	   while (refExchSymbADatabaseAccess.doQueryRowNext()) { // puts all columns of row to aADataColResultList
               
    		   sbmsg.append("Selected from "+refExchSymbADatabaseAccess.getThisTableName()+" ");
               for (ADataColResult aADataColResult:refExchSymbADatabaseAccess.rsADataColResultRowList) { //each col in this row
            	   sbmsg.append(" "+ aADataColResult.getColumnName()+"{"+aADataColResult.getColumnValue()+"}");	

                   if (aADataColResult.getColumnName().contentEquals("id")) {
                	   thisRefExchSymbId=aADataColResult.getColumnValue();	
                   }
               }
            }
               
            if (thisRefExchSymbId.isEmpty()) {
            	throw new AException(acomm, thisClassName+"=>Row NOT Found SQL{"+refExchSymbADatabaseAccess.getSqlStatementIn()+"}");
            }
            sbmsg.append(" RefExchSymbId{"+thisRefExchSymbId+"} ");
            //acomm.addPageMsgsLineOut(thisClassName+"=>Found=>"+refExchSymbADatabaseAccess.getThisTableName()+"=>"+sbmsg);        
           
           
       } catch (AExceptionSql e1) {
       	       throw  e1;
       }	
	   //
       fSymFundamentalId.setColumnValue(thisRefExchSymbId);
       sbmsg.setLength(0);
       sbmsg.append("");
	   try { 
 		   appADatabaseAccess.doProcessInsertRow(getDataColResultList());
 		   sbmsg.append(appADatabaseAccess.getThisTableName()+"{INSERTED] ");
       } catch (AExceptionSql e1) {
			  if (e1.isExceptionSqlRowDuplicate(acomm)) { //
				  setRowsInsertedDupsCtr(getRowsInsertedDupsCtr() + 1);
				  sbmsg.append(appADatabaseAccess.getThisTableName()
						      +"{Duplicate Not Inserted for ID{"+thisSymbCdValue+"}"
						      + "...msg{"+e1.getExceptionMsg()+"} ");				  
			   } else {
				   throw e1;
			   }
       }
	   //
	   try { 
		   refSectorADatabaseAccess.doProcessInsertRow(getDataColResultList());
 		   sbmsg.append(refSectorADatabaseAccess.getThisTableName()+"{INSERTED] ");
       } catch (AExceptionSql e1) {
			  if (e1.isExceptionSqlRowDuplicate(acomm)) { //
				  setRowsInsertedDupsCtr(getRowsInsertedDupsCtr() + 1);
				  sbmsg.append(refSectorADatabaseAccess.getThisTableName()
						      +"{Duplicate Not Inserted for ID{"+thisSymbCdValue+"}"
						      + "...msg{"+e1.getExceptionMsg()+"} ");				  
			   } else {
				   throw e1;
			   }
       }
	   //
	   //ADataColResult aRefExchSymbSectorId = new ADataColResult("", "");
	   
       try {    
    	   refSectorADatabaseAccess.doQuery("select id "
                   + " FROM ref_sector "
                   + " WHERE sector_nme = " +"'"+ fSectorNme.getColumnValue() +"'"
                   + "   AND industry_nme = " +"'"+ fIndustryNme.getColumnValue() +"'"
	                     );            
    	   thisRefSectorId="";
    	   while (refSectorADatabaseAccess.doQueryRowNext()) { // puts all columns of row to aADataColResultList
               
    		   sbmsg.append(" |Selected from "+refSectorADatabaseAccess.getThisTableName()+" ");
               for (ADataColResult aADataColResult:refSectorADatabaseAccess.rsADataColResultRowList) { //each col in this row
            	   sbmsg.append(" "+ aADataColResult.getColumnName()+"{"+aADataColResult.getColumnValue()+"}");	

                   if (aADataColResult.getColumnName().contentEquals("id")) {
                	   thisRefSectorId=aADataColResult.getColumnValue();	
                   }
               }
            }
               
            if (thisRefSectorId.isEmpty()) {
            	throw new AException(acomm, thisClassName+"=>Row NOT Found SQL{"+refSectorADatabaseAccess.getSqlStatementIn()+"}");
            }
            
            //acomm.addPageMsgsLineOut(thisClassName+"=>Found=>"+refExchSymbADatabaseAccess.getThisTableName()+"=>"+sbmsg);        
            
            int numupt = refExchSymbADatabaseAccess.doProcessUpdateRow(Arrays.asList(new ADataColResult("sector_id", thisRefSectorId))
            		                                   ,Arrays.asList(new ADataColResult("exch_cd", thisExchCdValue)
            		                                		         ,new ADataColResult("symb_cd", thisSymbCdValue)
            		                                		         )
            		);
            if (numupt==0) {
            	throw new AException(acomm, thisClassName+"=>Row NOT UPDATED{"
                                    +refExchSymbADatabaseAccess.getThisTableName()+"}"
                                    + " for " +" ExchCd{"+thisExchCdValue+"}"+" symb_cd{"+thisSymbCdValue+"}" + " update col sectorId{"+thisRefSectorId+"}"
                                    );
            }
            		
            sbmsg.append(" |UPDATED{"+refExchSymbADatabaseAccess.getThisTableName()+"}"
            		    +" #updt{"+numupt+"}" 
            		    +" sectorId{"+thisRefSectorId+"} for " +" ExchCd{"+thisExchCdValue+"}"+" symb_cd{"+thisSymbCdValue+"}"
                        );
            
       } catch (AExceptionSql e1) {
       	       throw  e1;
       }	
	   //
       fSymFundamentalId.setColumnValue(thisRefSectorId);	   
	   //
	   acomm.addPageMsgsLineOut("Row#{"+fileRowNum+"} exchCd{"+thisExchCdValue+"}"+" symbCd{"+thisSymbCdValue+"}"+" ["+sbmsg.toString()+"]");
	   //
	   fMsg.setColumnValue(sbmsg.toString());
	   //
	   //______________________________________________________________________________
	   //in case noting has been inserted/updated due to dups
		if (fileRowNum > appADatabaseAccess.commitAtNum) {
			doAppCommitControl(acomm, fileRowNum);
		}
	   //______________________________________________________________________________
		
	   //
  	   aFileExcelPOI.doOutputRowNext(acomm
			         , aSheetResult
			         , getDataRowColsValueList()
	     );  
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
		if (!isDataRowOut()) {
			   super.doDataRow(acomm, _exceptionSql, _isRowBreak);
		}
        //		
		return true; // or false to stop processing of file
        //
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
        //
        //
        appADatabaseAccess.doQueryRsExcel(aFileExcelPOI
                , "doQueryRsExcel "+appADatabaseAccess.getThisTableName()+" "
                , "Select *" 
                  +" from "+appADatabaseAccess.getThisTableName()+" " 
                 //+ " Where field_nme  = '" + ufieldname +"'" 
                  //+ " order by tab_name"
                 //+ " order by subject, topic, item"
                 );
		//
		appADatabaseAccess.doDbMetadataExcelSheet(aFileExcelPOI,appADatabaseAccess.getThisTableName()+" MetaData");
        //
		
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
        //
		//thisDataTrackAccess.doDbMetadataExcelSheet(aFileExcelPOI,"DataTrack MetaData");
		//
		appADatabaseAccess.doQueryRsExcel(aFileExcelPOI
		//thisDataTrackAccess.childDataTrackStoreAccess.doQueryRsExcel(aFileExcelPOI
                , "doQueryRsExcel data_track_store"
                , "Select *"
                  +" from data_track_store " 
                  + " Where source_nme  = '" + thisDataTrackAccess.getTrackFileName() +"'"
                 //+ " order by tab_name"
                 + " order by  run_start_ts desc, source_nme, source_mod_ts, data_track_id"
                 );
        //
		//thisDataTrackAccess.childDataTrackStoreAccess.doDbMetadataExcelSheet(aFileExcelPOI,"DataTrackStore MetaData");
    	//
   		try {
			aFileExcelPOI.doOutputEnd(acomm);
		} catch (IOException e) {
			throw new AException(acomm, e, " Close of outFileExcel");
		}
   		//
   		appADatabaseAccess.connectionCommit();
		appADatabaseAccess.connectionEnd();
		//
		refSectorADatabaseAccess.connectionCommit();
		refSectorADatabaseAccess.connectionEnd();
		//
		refExchSymbADatabaseAccess.connectionCommit();
		refExchSymbADatabaseAccess.connectionEnd();
		//
		thisDataTrackAccess.connectionCommit();
   		thisDataTrackAccess.connectionEnd();
   		//
   		
   		//
		//return super.doDataRowsEnded(acomm);
        return true;
		
	}
	
	/*
	 * 
	 */

	//	
	//
	// END
	//	
 }

