package hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Vinicius.Pires on 15/10/2015.
 */
public class OperatorMapRedReducer extends Reducer<Text,Text,NullWritable,Text>{

    public void reduce(Text chave, Iterable<Text> valores, Context context) throws IOException, InterruptedException {


        for (Text valor : valores) {
           context.write(NullWritable.get(),valor);
        }

    }


}
