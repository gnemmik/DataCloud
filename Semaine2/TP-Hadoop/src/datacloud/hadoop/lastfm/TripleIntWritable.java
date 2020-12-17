package datacloud.hadoop.lastfm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

//On definit notre type de valeur
	public class TripleIntWritable implements Writable {
		
		private IntWritable first;
		private IntWritable second;
		private IntWritable third;
		
		public TripleIntWritable() {
			set(new IntWritable(0), new IntWritable(0), new IntWritable(0));
		}
		
		public TripleIntWritable(IntWritable first, IntWritable second, IntWritable third) {
			set(first, second, third);
		}
		
		public TripleIntWritable(int first, int second, int third) {
			set(first, second, third);
		}
		
		public void set(IntWritable first, IntWritable second, IntWritable third) {
			this.first = first;
			this.second = second;
			this.third = third;
		}
		
		public void set(int first, int second, int third) {
			set(new IntWritable(first), new IntWritable(second), new IntWritable(third));
		}


		@Override
		public void readFields(DataInput in) throws IOException {
			first.readFields(in);
			second.readFields(in);
			third.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			first.write(out);
			second.write(out);
			third.write(out);
		}
		
		public IntWritable getFirst() {
			return first;
		}
		
		public IntWritable getSecond() {
			return second;
		}
		
		public IntWritable getThird() {
			return third;
		}
		
		@Override
		public String toString() {
			return this.first + "	" + this.second + "    " + this.third;
		}
		
	}

