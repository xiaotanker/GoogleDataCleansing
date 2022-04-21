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

        List<String[]> list = new ArrayList<>();
        for (Text value : values) {
            String str = value.toString();
            str = str + " ";
            String[] split = str.split(",");
            split[11] = split[11].substring(0, split[11].length() - 1);
            list.add(split);
        }

        Collections.sort(list, (s1,s2)->{
            LocalDate date1 = LocalDate.parse(s1[4]);
            LocalDate date2 = LocalDate.parse(s2[4]);
            return date1.compareTo(date2);
        });

        int[] last = new int[6];
        Arrays.fill(last,-1);
        for(int i=0; i<list.size(); i++){
            for(int j=0; j<6; j++){
                if(list.get(i)[j+5].length()==0){
                    continue;
                }

                if(last[j] == -1){
                    last[j] = i;
                    for(int k=0; k<i; k++){
                        list.get(k)[j+5] = list.get(i)[j+5];
                    }
                    continue;
                }

                double start = Double.parseDouble(list.get(last[j])[j+5]);
                double end = Double.parseDouble(list.get(i)[j+5]);
                double part = (end - start)/(i - last[j]);
                for(int k = last[j] + 1; k < i; k++){
                    list.get(k)[j+5] = String.format("%.2f", start + part * (k - last[j]));
                }
                last[j] = i;
            }
        }
        for(int i=0;i<6;i++){
            for(int j = last[i]+1; j < list.size();j++){
                list.get(j)[i+5] = list.get(last[i])[i+5];
            }
        }
        for(String[] split : list){
            context.write(NullWritable.get(), new Text(String.join(",",split)));
        }
    }
}
