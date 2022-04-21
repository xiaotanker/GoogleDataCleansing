import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDate;

public class FilterMapper
    extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context)
        throws IOException, InterruptedException {

        String line = value.toString();
        line = line + " ";

        String[] split = line.split(",");
        split[14] = split[14].substring(0,split[14].length()-1);

        if(split[0].equals("US")) {
            String output = String.join(",",split[1],split[2],split[3],split[7],split[8],split[9],split[10],split[11],split[12],split[13],split[14]);
            context.write(new Text(split[7]), new Text(output));
        }

    }

}

