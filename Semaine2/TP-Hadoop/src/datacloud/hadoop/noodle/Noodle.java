package datacloud.hadoop.noodle;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Noodle {
		
	public static class NoodleMapper extends Mapper<LongWritable, Text, TextText, TextIntWritable> {
		private String tranche;
		private String mois;
	      
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] space_split = value.toString().split(" ");
			String[] _split = space_split[0].split("_");
			String[] keywords_split = space_split[2].split("\\+");
			Boolean sended = false;
			
			if (Integer.parseInt(_split[4]) < 30) {
				tranche = "entre "+_split[3]+"h00 et "+ _split[3]+"h29";
			}else {
				tranche = "entre "+_split[3]+"h30 et "+ (Integer.parseInt(_split[3])+1)+"h00";
			}

			mois = (_split[1]);
			
			for (String m : keywords_split) {
				if(!sended) {
					context.write(new TextText(tranche, mois), new TextIntWritable(m, 1));
					sended = true;
				}else {
					context.write(new TextText(tranche, mois), new TextIntWritable(m, 0));
				}
			}
			
		}
	}	
	
	public static class NoodlePartitioner extends Partitioner<TextText, TextIntWritable> {

		@Override
		public int getPartition(TextText key, TextIntWritable value, int numPartitions) {
			return Integer.parseInt(key.getSecond().toString()) - 1;
		}
		
	}
	
	public static class NoodleReducer extends Reducer<TextText, TextIntWritable, Text, TextIntWritable> {
		
		public void reduce(TextText key, Iterable<TextIntWritable> values, Context context) throws IOException, InterruptedException {
			Map<String, Integer> map = new HashMap<>();
			String word = "";
			int max = 0;
			int nbReq = 0;
			
			for (TextIntWritable val : values) {
				if(map.containsKey(val.getFirst().toString())) {
					int tmp = map.get(val.getFirst().toString())+1;
					map.put(val.getFirst().toString(), tmp);
					
					if(tmp > max) {
						word = val.getFirst().toString();
						max = tmp;
					}
				}else {
					map.put(val.getFirst().toString(), 1);
					if(max == 0) {
						max = 1;
						word = val.getFirst().toString();
					}
				}
				nbReq += val.getSecond().get();
			}
			
			context.write(key.getFirst(), new TextIntWritable(word, nbReq));
		}
	}
	
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: noodle <in> <out>");
	      System.exit(2);
	    }
	    Job job = Job.getInstance(conf, "noodle");
	    job.setJarByClass(Noodle.class);//permet d'indiquer le jar qui contient l'ensemble des .class du job à partir d'un nom de classe
	    job.setMapperClass(NoodleMapper.class); // indique la classe du Mapper
	    job.setReducerClass(NoodleReducer.class); // indique la classe du Reducer
	    job.setMapOutputKeyClass(TextText.class);// indique la classe  de la clé sortie map
	    job.setMapOutputValueClass(TextIntWritable.class);// indique la classe  de la valeur sortie map    
	    job.setOutputKeyClass(Text.class);// indique la classe  de la clé de sortie reduce    
	    job.setOutputValueClass(TextIntWritable.class);// indique la classe  de la clé de sortie reduce
	    job.setInputFormatClass(TextInputFormat.class); // indique la classe  du format des données d'entrée
	    job.setOutputFormatClass(TextOutputFormat.class); // indique la classe  du format des données de sortie
	    job.setPartitionerClass(NoodlePartitioner.class);// indique la classe du partitionneur
	    job.setNumReduceTasks(12);// nombre de tâche de reduce : il est bien sur possible de changer cette valeur (1 par défaut)
	    
	    
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//indique le ou les chemins HDFS d'entrée
	    final Path outDir = new Path(otherArgs[1]);//indique le chemin du dossier de sortie
	    FileOutputFormat.setOutputPath(job, outDir);
	    final FileSystem fs = FileSystem.get(conf);//récupération d'une référence sur le système de fichier HDFS
		 if (fs.exists(outDir)) { // test si le dossier de sortie existe
			 fs.delete(outDir, true); // on efface le dossier existant, sinon le job ne se lance pas
		 }
	   
	    System.exit(job.waitForCompletion(true) ? 0 : 1);// soumission de l'application à Yarn
	  }

}
