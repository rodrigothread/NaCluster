package ch.epfl.codimsd.qeef.nacluster;

import ch.epfl.codimsd.qeef.nacluster.*;
import ch.epfl.codimsd.qeef.BlackBoard;
import ch.epfl.codimsd.qeef.DataUnit;
import ch.epfl.codimsd.qeef.Instance;
import ch.epfl.codimsd.qeef.Metadata;
import ch.epfl.codimsd.qeef.Operator;
import ch.epfl.codimsd.qeef.types.DoubleType;
import ch.epfl.codimsd.qeef.types.FloatType;
import ch.epfl.codimsd.qeef.types.OracleType;
import ch.epfl.codimsd.qeef.types.StringType;
import ch.epfl.codimsd.qeef.util.Constants;
import ch.epfl.codimsd.qep.OpNode;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/*
 * Esta classe é responsável por Identificar as tabelas que tenham tuplas modificadas
 * e também por montar o grafo de dependências das tabelas que serão copiadas. As tabelas
 * que devem ter seus dados copiados são inseridas no ArrayList e passadas para o operador
 * replicador.
 */
public class MapReduce extends Operator {

    private FileWriter fw;

    String classe = null;
    String dataset = null;
    String Programjar = null;
    String partitions = null;
    String numberofPartition=null;
    String result=null;

    //==========================================================================================
    // Construtores e funcoes utilizadas por ele
    //==========================================================================================

    public MapReduce(int id, OpNode opNode) throws Exception {
        super(id);
        
         Programjar = (String)(opNode.getParams()[0]);
        classe = (String)(opNode.getParams()[1]);
                 dataset = (String)(opNode.getParams()[2]);                
                 partitions=(String)(opNode.getParams()[3]);
                 result=(String)(opNode.getParams()[4]);
                 numberofPartition=(String)(opNode.getParams()[5]);
         //qefparameter=(String) (opNode.getParams()[0]);
    }

    //=========================================================================================
    //Overrrided functions
    //=========================================================================================
    public void open() throws Exception {

        super.open();

        InetAddress addr = InetAddress.getLocalHost();
        String hostname = addr.getHostName();

        String path = "/tmp/" + hostname;
        fw = new FileWriter(path);
        
 }

    public void setMetadata(Metadata prdMetadata[]) {
        //this.metadata[0] = (Metadata)prdMetadata[0].clone();
    }

    public void close() throws Exception {

        super.close();
    }

    public DataUnit getNext(int consumerId) throws Exception {

        Instance instance = (Instance) super.getNext(id);
        System.out.println("Map Reduce\n");
        //removed class name
        String[] command = {"/usr/local/hadoop/bin/hadoop", "jar",Programjar, dataset, partitions, result, numberofPartition};

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        Process p = processBuilder.start();
        p.waitFor();
        System.out.println("Operator MapReduce foi terminado com sucesso\n");

       // System.exit(0);
        return null;
    }
}
