/**
 * 
 */
package net.amaware.apps.marketsdata.load;

//import org.xml.sax.SAXException;
//import org.xml.sax.SAXParseException;
import java.io.IOException;

//import org.dom4j.Attribute;

import net.amaware.app.XmlApp;
import net.amaware.autil.ACommDb;
import net.amaware.autil.AException;
import net.amaware.autil.AFileO;
import net.amaware.autil.AXmlComponent;
import net.amaware.autil.AXmlElement;
import net.amaware.serv.HtmlTargetServ;
import net.amaware.serv.SourceProperty;
import org.xml.sax.*;
//import org.xml.sax.SAXException;
//import org.xml.sax.SAXParseException;
import org.jdom.*;
import org.jdom.input.SAXBuilder;
//
/**
 * @author PSDAA88 - Angelo M Adduci - Sep 6, 2005 3:02:12 PM
 * 
 */

//public class XmlFile extends DataStoreReport {
//public class XmlFile implements Serializable  {
public class ExchangeXML extends XmlApp {
	final  String thisClassName = this.getClass().getName();

	protected AFileO outXmlTxtFile = new AFileO();	
    protected String outXmlTxtFileName = "";
	
	/**
	 * 
	 */
	public ExchangeXML() {
		super();
	}

	public ExchangeXML(String _dbSchemaName) {
		//super(_dbSchemaName);
	}
	
	
//	public XmlApp processThis(ACommDb acomm, SourceProperty _aProperty, HtmlTargetServ _aHtmlServ) { 
//		super.processThis(acomm,_aProperty,_aHtmlServ); // MUST call super to process....or leave out for autoprocess

	public XmlApp processThis(ACommDb acomm, String _inFileNameFull, HtmlTargetServ _aHtmlServ) { 

		
		
		if (!outXmlTxtFile.isFileOpen()) {
			
	    	if (_inFileNameFull.endsWith(".xml")) {
	    		outXmlTxtFileName = _inFileNameFull.replace(".xml", ".txt");
			}
			if (_inFileNameFull.endsWith(".XML")) {
				outXmlTxtFileName = _inFileNameFull.replace(".XML", ".TXT");
			}

			try {			
			outXmlTxtFile.openFile(outXmlTxtFileName);
			outXmlTxtFile.writeLine("--============"
					+ acomm.getCurrTimestamp() + "===============");
			outXmlTxtFile.writeLine("-- "
					+ outXmlTxtFile.getThisFileNameFull());
			outXmlTxtFile
					.writeLine("--=====================================================");
			outXmlTxtFile.writeLine("//");
			_aHtmlServ.outTargetLine(acomm,
					"outXmlTxtFile Opened Name=" + outXmlTxtFileName);
			
			} catch (IOException e) {
				throw new AException(acomm, e, thisClassName
						+ "IOException"
				);
			}		
			
			

		}
	 
		super.processThis(acomm,_inFileNameFull,_aHtmlServ); // This will process XML file		
		
		return this;
	}
	
	
	public boolean processEnd(ACommDb acomm) {
		
		outXmlTxtFile.closeFile();
		
		return super.processEnd(acomm);
	}	
	
	
	/**
	 * returns with   
	 */
	 @Override
	public boolean doXmlRootBegin(ACommDb acomm, AXmlComponent _aXmlComponent) {
		 //super.doXmlRootBegin(acomm, _aXmlComponent); 
		 
		 outXmlTxtFile.writeLine("=>doXmlRootBegin"+" TagName{"+_aXmlComponent.getTagName()+"}"+" TagLev{"+_aXmlComponent.getTagLevel()+"}");		 
		 
		 
		return true; //return false to not add Root....did not try to see what happens if done.  
	 }

	/**
	 * invoked before attributes are loaded   
	 */
	@Override
	public boolean doXmlRootAttributesBegin(ACommDb acomm, AXmlComponent _aXmlComponent) {
		//super.doXmlRootAttributesBegin(acomm,_aXmlComponent); 		
				 
		return true;  
	}
	 
	 
	/**
	 * invoked  when no attributes were  found for this component   
	 */
	 @Override
	public boolean doXmlRootAttributeNone(ACommDb acomm, AXmlComponent _aXmlComponent) {
		 //super.doXmlRootAttributeNone(acomm,_aXmlComponent); 
		 
		return true;  
	 }

	/**
	 * invoked for each attribute of component, before it is added to list, allowing for changing of attibute)   
	 */
//	 @Override
//	public boolean doXmlRootAttributeNext(ACommDb acomm, AXmlComponent _aXmlComponent, Attribute _aAttribute) {
//		 //super.doXmlRootAttributeNext(acomm,_aXmlComponent,_aAttribute); 
		 
		 
//		return true;  
//	 }


     /**
	  * invoked for after last attribute was loaded   
	  */
	@Override
	public boolean doXmlRootAttributesEnd(ACommDb acomm, AXmlComponent _aXmlComponent) {
		//super.doXmlRootAttributesEnd(acomm,_aXmlComponent);		
				 
		return true;  
	}
	 
	/**
	* gets invoked for every attribute that was added to the component   
	*/
	 @Override
	public boolean doXmlRootEnd(ACommDb acomm, AXmlComponent _aXmlComponent) {
		 //super.doXmlRootEnd(acomm,_aXmlComponent);		 
			 
		 outXmlTxtFile.writeLine("=>doXmlRootEnd"+" TagName{"+_aXmlComponent.getTagName()+"}"+" TagLev{"+_aXmlComponent.getTagLevel()+"}");		 
		 
		return true; // return false if do NOT want to add this component 
	 }
	
	/**
	 * returns with   
	 */
	 @Override
	public boolean doXmlComponentBegin(ACommDb acomm, AXmlComponent _aXmlComponent) {
          super.doXmlComponentBegin(acomm,_aXmlComponent);
		 
		 outXmlTxtFile.writeLine("==>doXmlComponentBegin"+" TagName{"+_aXmlComponent.getTagName()+"}"+" TagLev{"+_aXmlComponent.getTagLevel()+"}");		 
		 
		return true;  //return false to not add Root....did not try to see what happens if done.
	 }

	/**
	 * invoked before attributes are loaded   
	 */
	 @Override
	public boolean doXmlComponentAttributesBegin(ACommDb acomm, AXmlComponent _aXmlComponent) {
		 super.doXmlComponentAttributesBegin(acomm, _aXmlComponent);
		 
		 outXmlTxtFile.writeLine(" ");
		 outXmlTxtFile.writeLine("===>doXmlComponentAttributesBegin"+" TagName{"+_aXmlComponent.getTagName()+"}"+" TagLev{"+_aXmlComponent.getTagLevel()+"}");
			 
		return true;  
	}
	 
	/**
	 * invoked  when no attributes were  found for this component   
	 */
	 @Override
	public boolean doXmlComponentAttributeNone(ACommDb acomm, AXmlComponent _aXmlComponent) {
		 super.doXmlComponentAttributeNone(acomm,_aXmlComponent);		
		 
		 outXmlTxtFile.writeLine("====>doXmlComponentAttributesNone"+" TagName{"+_aXmlComponent.getTagName()+"}"+" TagLev{"+_aXmlComponent.getTagLevel()+"}");
		 
		return true;  
	 }

	/**
	 * invoked for each attribute of component, before it is added to list, allowing for changing of attibute)   
	 */
	 @Override
	public boolean doXmlComponentAttributeNext(ACommDb acomm, AXmlComponent _aXmlComponent, Attribute _aAttribute) {
		 super.doXmlComponentAttributeNext(acomm,_aXmlComponent,_aAttribute);		 
		 
		 outXmlTxtFile.writeLine("====>doXmlComponentAttributeNext"+" TagName{"+_aXmlComponent.getTagName()+"}"+" TagLev{"+_aXmlComponent.getTagLevel()+"}");		 
		 
		 
		  return true;  
	 }

	/**
	 * invoked for after last attribute was loaded   
	 */
	 @Override
	public boolean doXmlComponentAttributesEnd(ACommDb acomm, AXmlComponent _aXmlComponent) {
		 super.doXmlComponentAttributesEnd(acomm,_aXmlComponent);		 
		 
		 outXmlTxtFile.writeLine("===>doXmlComponentAttributesEnd"+" TagName{"+_aXmlComponent.getTagName()+"}"+" TagLev{"+_aXmlComponent.getTagLevel()+"}");		 
		 
		  return true;  
	 }
	 

		/**
		 * gets invoked for every attribute that was added to the component   
		 */
	 @Override
	public boolean doXmlComponentEnd(ACommDb acomm, AXmlComponent _aXmlComponent) {
		 super.doXmlComponentEnd(acomm,_aXmlComponent);
		 
		 outXmlTxtFile.writeLine("==>doXmlComponentEnd"+" TagName{"+_aXmlComponent.getTagName()+"}"+" TagLev{"+_aXmlComponent.getTagLevel()+"}");
		 
		return true; // return false if do NOT want to add this component 
	}
	 
	 
	 
	 @Override
	public boolean doXmlElementNext(ACommDb acomm, AXmlComponent _aXmlComponent, AXmlElement _aXmlElement) {
		 super.doXmlElementNext(acomm,_aXmlComponent,_aXmlElement);		
		 outXmlTxtFile.writeLine("=====>doXmlElementNext"+" TagName{"+_aXmlComponent.getTagName()+"}"+" TagLev{"+_aXmlComponent.getTagLevel()+"}"
		 +" _aXmlElement{"+_aXmlElement.getTagName()+"}"+" Value{"+_aXmlElement.getTagDbValue()+"}");		 
		 
		  return true;  
	 }
	 
	/**
	 * invoked  when no attributes were  found for this component   
	 */
	 @Override
	public boolean doXmlElementAttributeNone(ACommDb acomm, AXmlComponent _aXmlComponent, AXmlElement _aXmlElement) {
		 super.doXmlElementAttributeNone(acomm,_aXmlComponent,_aXmlElement);		 

		 outXmlTxtFile.writeLine("======>doXmlElementAttributeNone"+" TagName{"+_aXmlComponent.getTagName()+"}"+" TagLev{"+_aXmlComponent.getTagLevel()+"}"
		 +" _aXmlElement{"+_aXmlElement.getTagName()+"}"+" Value{"+_aXmlElement.getTagDbValue()+"}");		 
		 
		 
		  return true;  
	 }

		/**
		 * invoked before any attribute was loaded   
		 */
	    @Override
		public boolean doXmlElementAttributesBegin(ACommDb acomm, AXmlComponent _aXmlComponent, AXmlElement _aXmlElement) {
	    	super.doXmlElementAttributesBegin(acomm, _aXmlComponent,_aXmlElement); // not needed    	

	    	outXmlTxtFile.writeLine(" ");
			 outXmlTxtFile.writeLine("======>doXmlElementAttributesBegin"+" TagName{"+_aXmlComponent.getTagName()+"}"+" TagLev{"+_aXmlComponent.getTagLevel()+"}"
					 +" _aXmlElement{"+_aXmlElement.getTagName()+"}"+" Value{"+_aXmlElement.getTagDbValue()+"}");		 
	    	
	    	
		  return true;  
	   }
	 
	 
	/**
     * invoked for each attribute of element.   
	 */
	@Override
	public boolean doXmlElementAttributeNext(ACommDb acomm, AXmlComponent _aXmlComponent
			                                              , AXmlElement _aXmlElement
				                                          , Attribute _aAttribute) {
		super.doXmlElementAttributeNext(acomm,_aXmlComponent, _aXmlElement, _aAttribute); 

		 outXmlTxtFile.writeLine("======>doXmlElementAttributeNext"+" TagName{"+_aXmlComponent.getTagName()+"}"+" TagLev{"+_aXmlComponent.getTagLevel()+"}"
		 +" _aXmlElement{"+_aXmlElement.getTagName()+"}"
		 +" _aAttribute{"+_aAttribute.getName()+"}" +"Val{"+_aAttribute.getValue()+"}"
		 +" Type{"+_aAttribute.getAttributeType()+"}" +" QualifiedName{"+_aAttribute.getQualifiedName()+"}");
		
		
		  return true; //return false if bypass adding   
	 }

	
	/**
	 * invoked for after last attribute was loaded   
	 */
    @Override
	public boolean doXmlElementAttributesEnd(ACommDb acomm, AXmlComponent _aXmlComponent, AXmlElement _aXmlElement) {
    	super.doXmlElementAttributesEnd(acomm,_aXmlComponent,_aXmlElement); 		 

		 outXmlTxtFile.writeLine("======>doXmlElementAttributesEnd"+" TagName{"+_aXmlComponent.getTagName()+"}"+" TagLev{"+_aXmlComponent.getTagLevel()+"}"
				 +" _aXmlElement{"+_aXmlElement.getTagName()+"}"+" Value{"+_aXmlElement.getTagDbValue()+"}");		 
    	
		 
	  return true;  
   }
	
	
	//
	// END
	//	
 }
