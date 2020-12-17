package datacloud.hadoop.noodle;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

//On definit notre type de valeur
	public class TextIntWritable implements Writable {
		
		private Text first;
		private IntWritable second;
		
		public TextIntWritable() {
			set(new Text(), new IntWritable());
		}
		
		public TextIntWritable(Text first, IntWritable second) {
			set(first, second);
		}
		
		public TextIntWritable(String first, int second) {
			set(new Text(first), new IntWritable(second));
		}
		
		private void set(Text first, IntWritable second) {
			this.first = first;
			this.second = second;
		}


		@Override
		public void readFields(DataInput in) throws IOException {
			first.readFields(in);
			second.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			first.write(out);
			second.write(out);
		}
		
		public Text getFirst() {
			return first;
		}
		
		public IntWritable getSecond() {
			return second;
		}
		
		@Override
		public String toString() {
			return this.first + "	" + this.second;
		}
		
	}
