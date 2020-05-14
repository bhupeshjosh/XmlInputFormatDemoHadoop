package com.bj.xml;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class CustomInputFormat extends TextInputFormat{

	@Override
	public RecordReader createRecordReader(InputSplit is, TaskAttemptContext tac) {
		
		return new XmlRecordReader();
	}
	
	
	public static class XmlRecordReader extends RecordReader<LongWritable, Text>{
		private LongWritable key;
		private Text value;
		private byte [] starttag;
		private byte [] endtag;
		private FSDataInputStream fsin ;
		private long start;
		private long end;
		DataOutputBuffer buff ;
		
		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			// close input sttream here.
			fsin.close();
			
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// return key
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// return value
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// get the current position of the stream reader
			return fsin.getPos();
		}

		@Override
		public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// initialise the reader using seek method at the start of the file
			FileSplit fsp=	(FileSplit)is;
			Path paths =	fsp.getPath();
			FileSystem files = paths.getFileSystem(tac.getConfiguration());
			fsin = files.open(paths) ;
			start = fsp.getStart();
			end = start + fsp.getLength();
			starttag = tac.getConfiguration().get("xmlstarttag").getBytes("utf-8");
			endtag = tac.getConfiguration().get("xmlendtag").getBytes("utf-8");
			buff = new DataOutputBuffer();
			key = new LongWritable();
			value = new Text();
			fsin.seek(start);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// if the end tag is found return true else return false
			if(readuntilmatch(starttag, false)) {
				try {
					buff.write(starttag);
					if(readuntilmatch(endtag, true)) {
						key.set(fsin.getPos());
						value.set(buff.getData(), 0, buff.getLength());
							return true;
					}					
				}
				finally {
					buff.reset();
				}

			}
			
			return false;
		}
		
		private boolean readuntilmatch(byte [] match, boolean writeToBuffer) throws IOException {
			int i =0 ;
			int length = match.length;
			int val =0;
			
			while(val != -1) {
				val = fsin.read();
				if(val == -1) {
					return false;
				}
				else {
					if(i == length) {
						return true;
					}
					else {
						if(val == match[i]) {
							i += 1;
							
							System.out.println("Match at position"+ fsin.getPos());
							
						}
						else {
							i = 0;
						}
					}
					
					if(writeToBuffer) buff.write(val);
				}
				
				
			}
			
			return false;
		}
		
	}
	
}