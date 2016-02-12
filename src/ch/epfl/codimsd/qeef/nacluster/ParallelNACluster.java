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
public class ParallelNACluster extends Operator {

    private FileWriter fw;

    String number2 = null;
    String dataset =null;
    String Programjar = null;
    String partitions = null;
    String number = null;
    String result = null;
    String conf = null;
    String conf2 =null;
    //String conf3=null;    //==========================================================================================
    // Construtores e funcoes utilizadas por ele
    //==========================================================================================
    public ParallelNACluster(int id, OpNode opNode) throws Exception {
        super(id);
        
        conf = (String) (opNode.getParams()[0]);
        conf2 = (String) (opNode.getParams()[1]);
        //conf3 = (String) (opNode.getParams()[2]);
        Programjar = (String) (opNode.getParams()[2]);
        dataset = (String) (opNode.getParams()[3]);
        result = (String) (opNode.getParams()[4]);
        partitions = (String) (opNode.getParams()[5]);
        number = (String) (opNode.getParams()[6]);     
        number2 = (String) (opNode.getParams()[7]);
    }

    //=========================================================================================
    //Overrrided functions
    //=========================================================================================
    public void open() throws Exception {

        super.open();

        System.out.println("OPEN PARALLEL");
        InetAddress addr = InetAddress.getLocalHost();
        String hostname = addr.getHostName();

        String path = "/tmp/" + hostname;
        fw = new FileWriter(path);
        
        BlackBoard bb = BlackBoard.getBlackBoard();
        String url = (String) bb.get("dataset");
        System.out.println("url = " + url);

    }

    public void setMetadata(Metadata prdMetadata[]) {
        //this.metadata[0] = (Metadata)prdMetadata[0].clone();
    }

    public void close() throws Exception {

        super.close();
    }

    public DataUnit getNext(int consumerId) throws Exception {

        
        
        Instance instance = (Instance) super.getNext(id);
        System.out.println("Parallel NACluster\n");
        //spark-submit --master spark://master:7077 /home/dexl/vini/ParallelNACluster.jar hdfs://mnmaster:9000/home/vini/saida hdfs://mnmaster:9000/home/vini/results /home/dexl/Project_QEF_1.0/data/partitions.txt 0 0
         String[] command = {"/usr/local/spark-1.5.0-bin-hadoop2.6/bin/spark-submit",conf,conf2,Programjar,dataset,result,partitions,number,number2};
        //String[] command = {"/usr/local/spark-1.5.0-bin-hadoop2.6/bin/spark-submit","--master","spark://master:7077",Programjar,"hdfs://146.134.232.10:9000/user/rodrigob/result","hdfs://mnmaster:9000/user/rodrigob/vini/results","/home/rodrigob/vini/catalogos/fronteiras.txt","0","0"};
        
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        Process p = processBuilder.start();
        p.waitFor();
        System.out.println("Operator Spark foi terminado com sucesso\n");

        //System.out.println(conf3);
        //System.out.println(Programjar);
        System.exit(0);
       
        return null;
    }
}
