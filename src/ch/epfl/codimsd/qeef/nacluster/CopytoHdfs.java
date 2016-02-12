package ch.epfl.codimsd.qeef.nacluster;


import ch.epfl.codimsd.qeef.BlackBoard;
import ch.epfl.codimsd.qeef.DataUnit;
import ch.epfl.codimsd.qeef.Instance;
import ch.epfl.codimsd.qeef.Metadata;
import ch.epfl.codimsd.qeef.Operator;
import ch.epfl.codimsd.qep.OpNode;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;


/*
 * Esta classe é responsável por Identificar as tabelas que tenham tuplas modificadas
 * e também por montar o grafo de dependências das tabelas que serão copiadas. As tabelas
 * que devem ter seus dados copiados são inseridas no ArrayList e passadas para o operador
 * replicador.
 */
public class CopytoHdfs extends Operator {

      
        //private DoubleType object;
        //hdfs 
       String fileFs;
       String configurationHdfs;
        //BD private OracleType object;
       Configuration conf;
       String fileinput;
       String fileinput2;
       String filewrite1;
       String filewrite2;
       
        
       
	//==========================================================================================
	// Construtores e funcoes utilizadas por ele
	//==========================================================================================
	public CopytoHdfs(int id, OpNode opNode) throws Exception{
		super(id);
                
               
                fileFs=(String) (opNode.getParams()[0]);
               configurationHdfs=(String) (opNode.getParams()[1]);
                fileinput=(String) (opNode.getParams()[2]);
                 fileinput2=(String) (opNode.getParams()[3]);
                  filewrite1=(String) (opNode.getParams()[4]);
                   filewrite2=(String) (opNode.getParams()[5]);
                    
	}

	//=========================================================================================
	//Overrrided functions
	//=========================================================================================
	public void open() throws Exception{

		super.open();
                
                InetAddress addr = InetAddress.getLocalHost();
                String hostname = addr.getHostName();
               
        
        }

        public void setMetadata(Metadata prdMetadata[]){
//                this.metadata[0] = (Metadata)prdMetadata[0].clone();
        }

	public void close() throws Exception{

		super.close();
	}

	public DataUnit getNext(int consumerId) throws Exception{

            Instance instance = (Instance)super.getNext(id);
            
            System.out.println("Copiando arquivos para HDFS\n");
          
          
                    
                conf = new Configuration();
              
               URI uri=new URI(configurationHdfs);
                FileSystem hdfs=FileSystem.get(uri,conf);
            
           
            //Print the home directory
               // System.out.println("Home folder -" +hdfs.getHomeDirectory());
               
           
            // Create
                Path workingDir=hdfs.getWorkingDirectory();
                Path newFolderPath= new Path("/Catalogos");
                Path newFolderPath2= new Path("/Catalogos_partitions");
                newFolderPath=Path.mergePaths(workingDir, newFolderPath);
                 newFolderPath2=Path.mergePaths(workingDir, newFolderPath2);
                if(hdfs.exists(newFolderPath)){
                
                 hdfs.delete(newFolderPath, true); //Delete existing Directory
                hdfs.delete(newFolderPath2, true);
                }                 
            //}
                hdfs.mkdirs(newFolderPath);
                hdfs.mkdirs(newFolderPath2);//Create new Directory
                System.out.println("A pasta com o nome data foi criada "+hdfs.getHomeDirectory());
             
            //Copying File from local to HDFS
            Path localFilePath = new Path(fileinput2);
            Path hdfsFilePath= new Path(newFolderPath+filewrite2);
             hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);  
            
            System.out.println("O arquivo foi copiado para o HDFS. "+hdfsFilePath); 
            
            Path localFilePath2 = new Path(fileinput);
            Path hdfsFilePath2= new Path(newFolderPath2+filewrite1);
             hdfs.copyFromLocalFile(localFilePath2, hdfsFilePath2);
             System.out.println("O arquivo foi copiado para o HDFS. "+hdfsFilePath2);
             System.out.println("Operator CopytoHdfs foi terminado com sucesso.\n");
                return null;
            }
                    
	}

