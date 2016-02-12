/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package ch.epfl.codimsd.qeef.linea;

import ch.epfl.codimsd.qeef.Acesso;
import ch.epfl.codimsd.qeef.DataUnit;
import ch.epfl.codimsd.qep.OpNode;

/**
 *
 * @author douglas
 */
public class PKLScan extends Acesso{
/**
	 * Construtor padr�o.
	 * @param id Identificador deste operador.
	 * @param dataSource Fonte de dados que ser� lida.
	 */
	public PKLScan(int id, OpNode op) {

	    super(id);

	}

	/**
	 * Realiza a inicializa��o da fonte de dados utilizada e define o formato das inst�ncias
	 * retornadas por este operador. O formato das inst�ncias ser� o mesmo que os da fonte de dados.
	 *
	 * @throws java.io.IOException Se algum erro acontecer durante a inicializa��o da fonte de dados.
	 */
	public void open() throws Exception{
	    //System.out.println("Scan open");
	    super.open();
	}

	/**
	 * Retorna a pr�xima inst�ncia da fonte de dados.
	 * @param consumerId Identificador do consumidor.
	 *
	 * @throws Exception Se acontecer alguma problema durante a leitura da fonte de dados.
	 */
	public DataUnit getNext(int consumerId) throws Exception{

	    instance = (dataSource).read();

            return instance;
	}

        /**
	 * Close the operator.
	 */
	public void close() throws Exception{

	    super.close();
	}
}
