import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FilterMapper
    extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context)
        throws IOException, InterruptedException {

        String line = value.toString();
        line = line + " ";

        String[] split = line.split(",");
        split[14] = split[14].substring(0,split[14].length()-1);

        if(split[0].equals("US")&&split[3].length()>0) {
            //US,State,county,month,kpi1,kpi2,kpi3,kpi4,kpi5,kpi6
            String output = String.join(",",split[1],split[2],split[3],split[8].substring(0,7),split[9],split[10],split[11],split[12],split[13],split[14]);


            context.write(new Text(String.join(",",split[7],split[8].substring(0,7))), new Text(output));
        }

    }

}

