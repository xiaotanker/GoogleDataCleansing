import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FilterMapper
    extends Mapper<LongWritable, Text, NullWritable, Text>{
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context)
        throws IOException, InterruptedException {

        String line = value.toString();
        String[] split = line.split(",");
        if(split[0].equals("US")&&split[3].length()>0) {
            String output = String.join(",",split[1],split[2],split[3],split[7],split[8],split[9],split[10],split[11],split[12],split[13],split[14]);
            context.write(NullWritable.get(), new Text(output));
        }
    }
}

