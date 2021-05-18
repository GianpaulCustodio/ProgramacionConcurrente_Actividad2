import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	  int sum=0; //almacenaremos el número de ocurrencias de cada una de las palabras correspondientes a la clave intermedia
	  for(IntWritable count : values){  
		  sum+=count.get(); //Realiza el contador
	  }
	  context.write(key,new IntWritable(sum)); //Recoger todas las veces que aparece la palabra y así realizar la suma.
  }
}