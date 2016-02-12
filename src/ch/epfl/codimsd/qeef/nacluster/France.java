package ch.epfl.codimsd.qeef.nacluster;

import ch.epfl.codimsd.qeef.linea.*;
import ch.epfl.codimsd.qeef.BlackBoard;
import ch.epfl.codimsd.qeef.DataUnit;
import ch.epfl.codimsd.qeef.Instance;
import ch.epfl.codimsd.qeef.Metadata;
import ch.epfl.codimsd.qeef.Operator;
import ch.epfl.codimsd.qeef.relational.Tuple;
import ch.epfl.codimsd.qeef.types.DoubleType;
import ch.epfl.codimsd.qeef.types.FloatType;
import ch.epfl.codimsd.qeef.types.IntegerType;
import ch.epfl.codimsd.qeef.types.OracleType;
import ch.epfl.codimsd.qeef.types.StringType;
import ch.epfl.codimsd.qeef.util.Constants;
import ch.epfl.codimsd.qep.OpNode;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;

/*
 * Esta classe é responsável por Identificar as tabelas que tenham tuplas modificadas
 * e também por montar o grafo de dependências das tabelas que serão copiadas. As tabelas
 * que devem ter seus dados copiados são inseridas no ArrayList e passadas para o operador
 * replicador.
 */
public class France extends Operator {

    BlackBoard bb = BlackBoard.getBlackBoard();
    String fileOut = null;
    String franceExe = null;
    String fileIn = null;
    String numberOfPartitions = null;
    String file = null;

	//==========================================================================================
    // Construtores e funcoes utilizadas por ele
    //==========================================================================================
    public France(int id, OpNode opNode) throws Exception {
        super(id);

        file = (String) bb.get("dataset");
        fileOut = (String) (opNode.getParams()[0]);
        franceExe = (String) (opNode.getParams()[1]);
        fileIn = (String) (opNode.getParams()[2]);
        numberOfPartitions = (String) (opNode.getParams()[3]);

    }

	//=========================================================================================
    //Overrrided functions
    //=========================================================================================
    public void open() throws Exception {

        super.open();

        System.out.println(file);

    }

    public void setMetadata(Metadata prdMetadata[]) {
        // this.metadata[0] = (Metadata)prdMetadata[0].clone();
    }

    public void close() throws Exception {

        super.close();
    }

    public DataUnit getNext(int consumerId) throws Exception {

        Tuple tuple = new Tuple();
        StringType fileInType, fileOutType;

        String command[] = {franceExe, fileIn, numberOfPartitions};

        Process proc = Runtime.getRuntime().exec(command);

        File file = new File(fileOut);

        // if file doesnt exists, then create it
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);

        // Read the output
        BufferedReader reader
                = new BufferedReader(new InputStreamReader(proc.getInputStream()));

        String line = "";

        while ((line = reader.readLine()) != null) {
            bw.write(line + "\n");
            bw.flush();
        }

        bw.close();

        System.out.println("Arquivo partitions foi criado.");

        proc.waitFor();

        fileInType = new StringType(fileIn);
        fileOutType = new StringType(fileOut);
        tuple.addData(fileInType);
        tuple.addData(fileOutType);

        System.out.println("Operator France foi terminado com sucesso\n");

        return tuple;

    }

}
