import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * @author Jakob Runge
 * Copies files from a hard coded source location
 * to a hardcoded target location.
 * Source and target are directories,
 * and the source directory is iterated for files to be copied.
 * */
public class HdfsInsert {
	/**
	 * All there is to do happens in here.
	 * */
	public static void main(String args[]){
		// Setup of some basic variables to be used:
		// The hard coded source directory:
		File srcDir = new File("/home/hadoop/praktikum/");
		/*
		 * The FileSystem handle.
		 * It is left null, because we want to access it outside
		 * the try block that connects to the dfs.
		 * */
		FileSystem fs = null;
		//How we connect to the dfs:
		String fsBase = "hdfs://localhost:8019";
		//Handling the connection attempt:
		try {
			fs = FileSystem.get(new URI(fsBase), new Configuration());
		} catch (Exception e) {
			System.out.println("Failed with FileSystem.get()");
			e.printStackTrace();
		}
		System.out.println("Connected to dfs :)");
		//Creating target directory in the dfs:
		String targetDir = "/input_data/praktikum/";
		System.out.println("Creating target directory: " + targetDir);
		try {
			fs.mkdirs(new Path(targetDir));
		} catch (IOException e1) {
			System.out.println("Could not create target directory.");
			e1.printStackTrace();
		}
		//We add the targetDir to the fsBase to copy files there:
		fsBase += targetDir;
		//Iterating files in srcDir:
		System.out.println("Iterating files…");
		for(File f : srcDir.listFiles()){
			try {
			System.out.println("Copy file: " + f.getName() + " …");
			Path source = new Path(f.getCanonicalPath());
			Path target = new Path(fsBase + f.getName());
			fs.copyFromLocalFile(source, target);
			} catch (IOException e) {
				System.out.println("Problem copying file: " + f.getName());
				e.printStackTrace();
			}
		}
		System.out.println("…aaand it's done :D");
	}
}