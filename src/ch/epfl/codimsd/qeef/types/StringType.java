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
/*
 * Created on Mar 9, 2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ch.epfl.codimsd.qeef.types;
        
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;
import org.apache.log4j.Logger;

import ch.epfl.codimsd.qeef.Data;


/**
 * Classe especializada de Type que define o tipo String na m�quina de execu��o.
 * 
 * @author Vinicius Fontes
 *
 * @date Mar 9, 2005
 */
@SuppressWarnings("serial")
public class StringType implements Type, Serializable {
    
	/**
	 * Log4j logger.
	 */
	@SuppressWarnings("unused")
	private static Logger logger = Logger.getLogger(StringType.class.getName());
	
    /**
     * Define uma String. Aceita qualquer sequ�ncia de caracter que come�e com "
     * termine com " e n�o contenha " no meio
     */
    public static final String STRING_PATTERN = "([\"][\\p{ASCII}][^\"]*[\"])";
    
    private String value;
    
    //------------------------------------------------------------
    /**
     * Construtor pdr�o.
     */
    public StringType(){
        super();
    }
    
    public StringType(String value){
        this.value = value;
    }
    

    public void setValue(String value){
        
        if( value.startsWith("\""))
            value = value.substring(1);
        
        if( value.endsWith("\""))
            value = value.substring(0, value.length()-1);
        
        this.value = value;
    }
    
    public void setValue(Object value){
        setValue((String)value);
    }

    
    /**
     * Define um mecanismo de ordena��o entre Strings.
     * 
     * @param o Uma String a ser comparada com o valor desta inst�ncia.
     * 
     * @return Um valor inteiro que dir� se e 
     */
    /* Author Othman Tajmouati
     * "return value.compareTo(o);" has been removed and replaced by "return 0;" due to undefined errors
     */ 
    public int compareTo(Object o) {
       
        //return value.compareTo(o);
    	return 0;
    }
    
	public Object clone(){
	    StringType v = new StringType();
	    v.value = new String(this.value);
	    return v;
	}
    
    
    public String toString(){
        return (value);
    }

    /*
     * 
     */
    public void display(Writer out) throws IOException{
        out.write(value);
        out.write(" ");
    }
    
    /*
     * 
     */    
    public int displayWidth() {
        return 12;
    }
    
    /*
     * 
     */
    public Type newInstance(){
        return new StringType();
    }


    /*
     * 
     */
    public Type read(DataInputStream in) throws IOException{

    	return new StringType( in.readUTF() );  
    }

    /*
     * 
     */
    public void write(DataOutputStream out) throws IOException{
    
    	out.writeUTF(value);
    }

    /**
     * @see Type#recognitionPattern()
     */
    public String recognitionPattern(){
        
        return STRING_PATTERN;
    }
    
    /**
     *  A implementa��o deste m�todo default da linguagem se tornou obrigat�ria
     * com o objetivo de agilizar o processo de libera��o de mem�ria que �
     * realizado no Garbage Colector. Recomenda-se atribuir null a todas as suas refer�ncias.
     */
    public void finalize() throws Throwable{
        super.finalize();
        value = null;
    }
    
    public void setMetadata(Data data){}
}

