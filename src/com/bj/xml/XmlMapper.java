package com.bj.xml;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import java.util.logging.Level;
import java.util.logging.Logger;

public class XmlMapper extends Mapper<LongWritable,Text,NullWritable,Text>{
	
	public void Map(LongWritable key, Text value , Context context) throws InterruptedException {
		String xmlstring = value.toString();
		SAXBuilder builder = new SAXBuilder();
		Reader in = new StringReader(xmlstring);
		
		try {
			Document doc = builder.build(in);
			Element root = doc.getRootElement();
			
			String tag1 = root.getChild("name").getTextTrim();
			String tag2 = root.getChild("value").getTextTrim() ;
			context.write(NullWritable.get(), new Text(tag1+ ","+tag2));
		}
		catch (JDOMException ex)
		{
			Logger.getLogger(XmlMapper.class.getName()).log(Level.SEVERE, null, ex);
		}
		catch (IOException ex)
		{
			Logger.getLogger(XmlMapper.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}
