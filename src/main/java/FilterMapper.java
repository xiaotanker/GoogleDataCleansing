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
        String from = split[0];
        if(split[0].equals("US")&&split[3].length()>0) {
            context.write(NullWritable.get(), new Text(line));
        }
    }
}

