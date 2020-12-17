package datacloud.hadoop.noodle;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


// On definit notre type de cle
public class TextText implements WritableComparable<TextText> {
	
	private Text first;
	private Text second;
	
	public TextText() {
		set(new Text(), new Text());
	}
	
	public TextText(Text first, Text second) {
		set(first, second);
	}
	
	public TextText(String first, String second) {
		set(new Text(first), new Text(second));
	}
	
	private void set(Text first, Text second) {
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

	@Override
	public int compareTo(TextText mt) {
		int cmp = first.compareTo(mt.first);
		if(cmp != 0) {
			return cmp;
		}
		
		return second.compareTo(mt.second);
	}
	
	public Text getFirst() {
		return first;
	}
	
	public Text getSecond() {
		return second;
	}
	
}
