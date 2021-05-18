import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCounterDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
	  int res = ToolRunner.run((org.apache.hadoop.util.Tool) new WordCounterDriver(), args);
	  System.exit(res);
  }
  public int run(String[] args) throws Exception{
	  Job job = Job.getInstance(getConf(), "WordCounter"); //Creamos una nueva instancia y le asignamos un nombre
	  job.setJarByClass(this.getClass()); //Asignamos la clase a la que va a llamar la funcion
	  FileInputFormat.addInputPath(job, new Path(args[0])); //Path de entrada
	  FileOutputFormat.setOutputPath(job, new Path(args[1])); //Path de salida
	  
	  job.setMapperClass(WordCounterMapper.class); //Se encargará del proceso Mapper
	  job.setReducerClass(WordCounterReducer.class); //Se encargará del proceso Reducer
	  
	  job.setOutputKeyClass(Text.class); //Tipo Text de Hadoop
	  job.setOutputValueClass(IntWritable.class); //Valor de la tupla de salida
	  
	  return job.waitForCompletion(true) ? 0:1; //Indicar el resultado de la ejecución... 0:Todo OK -- 1:Caso Contrario
  }
}

