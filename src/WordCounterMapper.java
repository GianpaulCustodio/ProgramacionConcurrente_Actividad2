import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1); //Clase propia para definir enteros en Hadoop
	private final static Pattern SPLIT_PATTERN = Pattern.compile("\\s*\\b\\s*"); //Alcenaremos el patron, por lo que vamos a dividir cada linea del texto. Vamos a dividir por TOKENS
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String line = value.toString(); //El valor de la línea de texto se convierte en la clase Text que es el string distribuido de Haddop
	  line = line.replace("[^a-zA-Z0-9 ]", ""); //Nos quedaremos con palabras, es decir, eliminamos simbolos no deseados
	  Text currentWord = new Text(); //Almacenaremos cada una de las palabras con la que vamos a crear las claves intermedias
	  
	  String words[] = SPLIT_PATTERN.split(line); //Array de strings donde vamos a meter cada una de las palabras
	  for(int i=0;i<words.length;i++){ //Se encargará de emitir las claves y los valores intermedios
		  if(words[i].isEmpty()){ //Veremos si es un espacio
			  continue;
		  }
		  currentWord = new Text(words[i]); //Si la palabra actual es igual a palabra que esté contenida en el array de palabras. String JAVA -> String HADOOP
		  context.write(currentWord, one); //Escribiremos en el contexto de la aplicacion
		  
	  }
  }
}
