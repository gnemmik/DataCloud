package datacloud.hadoop.lastfm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LastFM {
	
	public static class LastFMMapper1 extends Mapper<LongWritable, Text, IntWritable, TripleIntWritable> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			List<String> str = new ArrayList<>();
		    while (itr.hasMoreTokens()) {
		        str.add(itr.nextToken());
		    }
			
			IntWritable trackId = new IntWritable(Integer.parseInt(str.get(1)));
			int local = Integer.parseInt(str.get(2));
			int radio = Integer.parseInt(str.get(3));
			int listener = local + radio;
			
			if(listener > 0) {
				context.write(trackId, new TripleIntWritable(listener, 0, 0));
			}
		}
	}
	
	public static class LastFMMapper2 extends Mapper<LongWritable, Text, IntWritable, TripleIntWritable> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			List<String> str = new ArrayList<>();
		    while (itr.hasMoreTokens()) {
		        str.add(itr.nextToken());
		    }
			
			IntWritable trackId = new IntWritable(Integer.parseInt(str.get(1)));
			int local = Integer.parseInt(str.get(2));
			int radio = Integer.parseInt(str.get(3));
			int listening = local + radio;
			int skip = Integer.parseInt(str.get(4));
			
			context.write(trackId, new TripleIntWritable(0, listening, skip));
		}
	}
	

	public static class LastFMMapper3 extends Mapper<LongWritable, Text, IntWritable, TripleIntWritable> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			List<String> str = new ArrayList<>();
		    while (itr.hasMoreTokens()) {
		        str.add(itr.nextToken());
		    }
			
			IntWritable trackId = new IntWritable(Integer.parseInt(str.get(0)));
			int listener = Integer.parseInt(str.get(1));
			int listening = Integer.parseInt(str.get(2));
			int skip = Integer.parseInt(str.get(3));
			
			context.write(trackId, new TripleIntWritable(listener, listening, skip));
		}
	}
	
	
	
	public static class LastFMReducer1 extends Reducer<IntWritable, TripleIntWritable, IntWritable, TripleIntWritable> {
		
	    public void reduce(IntWritable key, Iterable<TripleIntWritable> values, Context context) throws IOException, InterruptedException {
	    	int listener = 0;
	    	for (@SuppressWarnings("unused") TripleIntWritable val : values) {
	    		listener++;
	    	}
	    	context.write(key, new TripleIntWritable(listener, 0, 0));
	    }
	}
	
	public static class LastFMReducer2 extends Reducer<IntWritable, TripleIntWritable, IntWritable, TripleIntWritable> {

	    public void reduce(IntWritable key, Iterable<TripleIntWritable> values, Context context) throws IOException, InterruptedException {
	    	TripleIntWritable result = new TripleIntWritable(0,0,0);
	    	for (TripleIntWritable val : values) {
	    		result.set(0, result.getSecond().get()+val.getSecond().get(), result.getThird().get()+val.getThird().get());
	    	}
	    	context.write(key, result);
	    }
	}
	
	
	
	public static class LastFMReducer3 extends Reducer<IntWritable, TripleIntWritable, IntWritable, TripleIntWritable> {

	    public void reduce(IntWritable key, Iterable<TripleIntWritable> values, Context context) throws IOException, InterruptedException {
	    	int listener = 0;
	    	int listening = 0;
	    	int skip = 0;
	    	
	    	for (TripleIntWritable val : values) {
	    		//result.set(result.getFirst().get()+val.getFirst().get(), result.getSecond().get()+val.getSecond().get(), result.getThird().get()+val.getThird().get());
	    		listener += val.getFirst().get();
	    		listening += val.getSecond().get();
	    		skip += val.getThird().get();
	    	}
	    	context.write(key, new TripleIntWritable(listener, listening, skip));
	    }
	}
	
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    
	    Job job = Job.getInstance(conf, "last fm");
	    
	    if(otherArgs[0].equals("1")) {
	    	
	    	if (otherArgs.length != 3) {
	  	      System.err.println("Usage: LastFM <numjob> <in> <out>");
	  	      System.exit(2);
	  	    }
	    	
	        job.setJarByClass(LastFM.class);//permet d'indiquer le jar qui contient l'ensemble des .class du job à partir d'un nom de classe
		    job.setMapperClass(LastFMMapper1.class); // indique la classe du Mapper
		    job.setReducerClass(LastFMReducer1.class); // indique la classe du Reducer
		    job.setMapOutputKeyClass(IntWritable.class);// indique la classe  de la clé sortie map
		    job.setMapOutputValueClass(TripleIntWritable.class);// indique la classe  de la valeur sortie map    
		    job.setOutputKeyClass(IntWritable.class);// indique la classe  de la clé de sortie reduce    
		    job.setOutputValueClass(TripleIntWritable.class);// indique la classe  de la clé de sortie reduce
		    job.setInputFormatClass(TextInputFormat.class); // indique la classe  du format des données d'entrée
		    job.setOutputFormatClass(TextOutputFormat.class); // indique la classe  du format des données de sortie
		    //job.setPartitionerClass(HashPartitioner.class);// indique la classe du partitionneur
		    job.setNumReduceTasks(1);// nombre de tâche de reduce : il est bien sur possible de changer cette valeur (1 par défaut)
		    
		    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));//indique le ou les chemins HDFS d'entrée
		    final Path outDir = new Path(otherArgs[2]);//indique le chemin du dossier de sortie
		    FileOutputFormat.setOutputPath(job, outDir);
		    final FileSystem fs = FileSystem.get(conf);//récupération d'une référence sur le système de fichier HDFS
			if (fs.exists(outDir)) { // test si le dossier de sortie existe
				fs.delete(outDir, true); // on efface le dossier existant, sinon le job ne se lance pas
			}
			System.exit(job.waitForCompletion(true) ? 0 : 1);// soumission de l'application à Yarn
	    }
	    
	    else if(otherArgs[0].equals("2")) { 
	    	
	    	if (otherArgs.length != 3) {
	  	      System.err.println("Usage: LastFM <numjob> <in> <out>");
	  	      System.exit(2);
	  	    }
	    	
		    job.setJarByClass(LastFM.class);//permet d'indiquer le jar qui contient l'ensemble des .class du job à partir d'un nom de classe
		    job.setMapperClass(LastFMMapper2.class); // indique la classe du Mapper
		    job.setReducerClass(LastFMReducer2.class); // indique la classe du Reducer
		    job.setMapOutputKeyClass(IntWritable.class);// indique la classe  de la clé sortie map
		    job.setMapOutputValueClass(TripleIntWritable.class);// indique la classe  de la valeur sortie map    
		    job.setOutputKeyClass(IntWritable.class);// indique la classe  de la clé de sortie reduce    
		    job.setOutputValueClass(TripleIntWritable.class);// indique la classe  de la clé de sortie reduce
		    job.setInputFormatClass(TextInputFormat.class); // indique la classe  du format des données d'entrée
		    job.setOutputFormatClass(TextOutputFormat.class); // indique la classe  du format des données de sortie
		    //job.setPartitionerClass(HashPartitioner.class);// indique la classe du partitionneur
		    job.setNumReduceTasks(1);// nombre de tâche de reduce : il est bien sur possible de changer cette valeur (1 par défaut)
		    
		    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));//indique le ou les chemins HDFS d'entrée
		    final Path outDir = new Path(otherArgs[2]);//indique le chemin du dossier de sortie
		    FileOutputFormat.setOutputPath(job, outDir);
		    final FileSystem fs = FileSystem.get(conf);//récupération d'une référence sur le système de fichier HDFS
			if (fs.exists(outDir)) { // test si le dossier de sortie existe
				fs.delete(outDir, true); // on efface le dossier existant, sinon le job ne se lance pas
			}
			System.exit(job.waitForCompletion(true) ? 0 : 1);// soumission de l'application à Yarn
	    }
	    
	    else if(otherArgs[0].equals("3")) { 
	    	
	    	if (otherArgs.length != 4) {
	  	      System.err.println("Usage: LastFM <numjob> <in_1> <in_2> <out>");
	  	      System.exit(2);
	  	    }
	    	job.setJarByClass(LastFM.class);//permet d'indiquer le jar qui contient l'ensemble des .class du job à partir d'un nom de classe
		    job.setMapperClass(LastFMMapper3.class); // indique la classe du Mapper
		    job.setReducerClass(LastFMReducer3.class); // indique la classe du Reducer
		    job.setMapOutputKeyClass(IntWritable.class);// indique la classe  de la clé sortie map
		    job.setMapOutputValueClass(TripleIntWritable.class);// indique la classe  de la valeur sortie map    
		    job.setOutputKeyClass(IntWritable.class);// indique la classe  de la clé de sortie reduce    
		    job.setOutputValueClass(TripleIntWritable.class);// indique la classe  de la clé de sortie reduce
		    job.setInputFormatClass(TextInputFormat.class); // indique la classe  du format des données d'entrée
		    job.setOutputFormatClass(TextOutputFormat.class); // indique la classe  du format des données de sortie
		    //job.setPartitionerClass(HashPartitioner.class);// indique la classe du partitionneur
		    job.setNumReduceTasks(1);// nombre de tâche de reduce : il est bien sur possible de changer cette valeur (1 par défaut)
		    
	    	MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, LastFMMapper3.class);
	    	MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class, LastFMMapper3.class);
	    	final Path outDir = new Path(otherArgs[3]);//indique le chemin du dossier de sortie
		    FileOutputFormat.setOutputPath(job, outDir);
		    final FileSystem fs = FileSystem.get(conf);//récupération d'une référence sur le système de fichier HDFS
			if (fs.exists(outDir)) { // test si le dossier de sortie existe
				fs.delete(outDir, true); // on efface le dossier existant, sinon le job ne se lance pas
			}
			System.exit(job.waitForCompletion(true) ? 0 : 1);// soumission de l'application à Yarn
	    }
	    
	    else {
	    	System.err.println("Usage : <numjob> in [1, 2, 3]");
	    	System.exit(2);
		}
	    
	    
	    
	   
	    
	  }

}
