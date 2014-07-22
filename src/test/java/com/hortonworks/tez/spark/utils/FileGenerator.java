package com.hortonworks.tez.spark.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

public class FileGenerator {
	
	public static void main(String[] args) throws Exception {
		generateFile(new File("sample-256.txt"));
	}
	
	public static void generateFile(File outputFile) throws Exception {
		//268435456
		File inputFile = new File("foo.txt");
//		File outputFile = new File("sample-256.txt");
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
		String line;
		int counter = 0;
		while (counter < 268435456){
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			while ((line = reader.readLine()) != null) {
				writer.write(line);
				writer.write("\n");
				counter += (line.length()+1);
			}
			reader.close();
		}
		writer.close();
	}

}
