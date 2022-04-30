import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

public class FillingReducer
        extends Reducer<Text, Text, NullWritable, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //value :US,State,county,month,kpi1,kpi2,kpi3,kpi4,kpi5,kpi6
        List<String[]> list = new ArrayList<>();
        for (Text value : values) {
            String str = value.toString();
            str = str + " ";
            String[] split = str.split(",");
            split[9] = split[9].substring(0, split[9].length() - 1);
            list.add(split);
        }
        double[] sum = new double[6];
        int[] count = new int[6];
        for(String[] split: list){
            for(int i = 4; i <= 9;i++){
                if(split[i].length()>0){
                    count[i-4]++;
                    sum[i-4]+=Double.parseDouble(split[i]);
                }
            }
        }
        String[] first = list.get(0);
        String out = String.join(",",first[0],first[1],first[2],first[3],
                count[0]==0?"":String.format("%.2f",sum[0]/count[0]),
                count[1]==0?"":String.format("%.2f",sum[1]/count[1]),
                count[2]==0?"":String.format("%.2f",sum[2]/count[2]),
                count[3]==0?"":String.format("%.2f",sum[3]/count[3]),
                count[4]==0?"":String.format("%.2f",sum[4]/count[4]),
                count[5]==0?"":String.format("%.2f",sum[5]/count[5]));
        context.write(NullWritable.get(),new Text(out));

    }
}
