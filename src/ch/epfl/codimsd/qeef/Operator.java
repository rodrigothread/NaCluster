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
package ch.epfl.codimsd.qeef;

import java.util.*;

import org.apache.log4j.Logger;

/**
 * Abstract class that defines commons functions and structrues for all CoDIMS operators.
 * In this version, it was opted to eliminate the AlgebraicOperator and ControlOperator in order
 * to prevent a long chain of operators causing long execution time. For same reason, the method hasNext was
 * removed. Here are the functionalities of the class :
 * <li> Managing communication between consumers and producers.
 * <li> Controling the number of tuples produced.
 * <li> Capturing pictures of communication between operators of a plan.
 * 
 * Added by Othman : 
 * - Javadoc translated to english.
 * - Constructor changed, from Operator(int id, BlackBoard blackBoard) to Operator(int id)
 * - Added method returnMetadata().
 * 
 * @author Fausto Ayres, Vinicius Fontes, Othman Tajmouati
 */

public abstract class Operator {

    /**
     * Id of the operator.  
     */
    protected int id = 0;

    /**
     * List of consumers.
     */
    protected LinkedList<Operator> consumers;

    /**
     * List of producers.
     */
    protected LinkedList<Operator> producers;

    /**
     * Number of tuples produced.
     */
    protected int produced = 0;
    
    /**
     * Number of tuples consumed.
     */
    protected int consumed = 0;

    /**
     * Description of the format of the tuples that this operator produces, one for each consumer. 
     */
    protected Metadata metadata[];

    /**
     * Last tuple produced. 
     */
    protected DataUnit instance;

    /**
     * Log4j logger.
     */
    @SuppressWarnings("unused")
	private static Logger logger = Logger.getLogger(Operator.class.getName());

    
    /**
     * Flag indicating if the operator has more tuples to be consumed.
     */
    protected boolean hasNext;

    /**
     * Default constructor.
     * 
     * @param id identifier of the operator.
     */
    public Operator(int id) {

    	this.id = id;
    	this.produced = 0;
    	metadata = null;
    	consumers = new LinkedList<Operator>();
    	producers = new LinkedList<Operator>();
    }

    /**
     * Opens the list of producers and set the metadata for this operator.
     * 
     * @throws Exception
     */
    public void open() throws Exception {

        Operator op;
        metadata = new Metadata[getProducers().size()];
        Metadata aux[] = new Metadata[getProducers().size()];
        hasNext = true;

        // Open the list of producers.
        for (int i = 0; i < producers.size(); i++) {
        	
            // Open the operator.
            op = (Operator) producers.get(i);
            op.open();
            logger.debug("OP(" + id + ") open.");

            // Get producers metadatas.
            aux[i] = op.getMetadata(id);
            
        }
        
        // Set the metadata for this operator.
        setMetadata(aux);
    }

    /**
     * Close the operator. It closes the list of producers.
     * 
     * @throws Exception
     */
    public void close() throws Exception {

        Iterator enumProd;
        enumProd = producers.iterator();
        while (enumProd.hasNext()) {

            ((Operator) enumProd.next()).close();
        }
        
        consumers = null;
        producers = null;
        metadata = null;
        instance = null;
        
        // logger.debug("OP(" + id + "): closed");
    }

    /**
     * Consume a tuple.
     * 
     * @param consumerId the consumer id.
     * @return the tuple consumed.
     * @throws Exception
     */
    public DataUnit getNext(int consumerId) throws Exception {
    	
        if(!hasNext)
            return null;

        instance = getProducer(0).getNext(id);
        if (instance!= null)
            produced ++;
        else
            hasNext = false;
                
        return instance;
    }

    /**
     * @return the list of consumers.
     */
    public Collection getConsumers() {
        return consumers;
    }

    /**
     * Set the consumers.
     *  
     * @consumers Colletion of consumers.
     */
    public void setConsumers(Collection<Operator> consumers) {
        this.consumers = new LinkedList<Operator>(consumers);
    }

    /**
     * Add a consumer to the list of consumers.
     * 
     * @param op the operator to add.
     */
    public void addConsumer(Operator op) {
        consumers.addLast(op);
    }

    /**
     * @param nth the consumer id
     * @return consumer at position n in the list of consumers.
     */
    public Operator getConsumer(int nth) {
        return ((Operator) consumers.get(nth));
    }

    /**
     * @return List of producers.
     */
    public Collection getProducers() {
        return producers;
    }

    /**
     * Add a producer to the list of producers.
     * 
     * @param op the producer to add.
     */
    public void addProducer(Operator op) {
        producers.add(op);
    }

    /**
     * @param nth position of the producer.
     * @return producer at position n in the list of producers.
     */
    public Operator getProducer(int nth) {
        return ((Operator) producers.get(nth));
    }

    /**
     * @see Object#toString()
     */
    public String toString() {
        return "OP(" + id + ") produced " + produced;
    }

    /**
     * @return the opeator id.
     */
    public int getId() {
        return id;
    }

    /**
     * @param idConsumer consumer id
     * @return Metadados Return the metadata of a consumer operator
     * 
     */
    public Metadata getMetadata(int idConsumer) {
                
        ListIterator itConsumers;
        Operator currOp;
        
        if(consumers.size()==0)
        {
            return metadata[0];
        }
        else {
	        itConsumers = consumers.listIterator();
	        while(itConsumers.hasNext()){
	            currOp = (Operator)itConsumers.next();
	            if(currOp.id == idConsumer)
	                return metadata[itConsumers.nextIndex()-1];
	        }
        }
        
        return null;
    }
    
    /**
     * Abstract class for setting the metadata. Each operator should know how to define its
     * own metadatas.
     * 
     * @param metadata the metadata of the operator.
     */
    public abstract void setMetadata(Metadata metadata[]);
    
    /**
     * @return the metadata for QEEF.
     */
    public Metadata[] returnMetadata() {
    	return metadata;
    }
}

