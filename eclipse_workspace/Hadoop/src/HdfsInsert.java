import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/***/
public class HdfsInsert {
	/***/
	public static void main(String args[]){
		//Setup foo:
		File srcDir = new File("/home/hadoop/praktikum/");
		FileSystem fs = null;
		String fsBase = "hdfs://localhost:8019";
		//Connecting to the fs:
		try {
			fs = FileSystem.get(new URI(fsBase), new Configuration());
		} catch (Exception e) {
			System.out.println("Failed with FileSystem.get()");
			e.printStackTrace();
		}
		System.out.println("Connected to dfs :)");
		//Creating target directory:
		String targetDir = "/input_data/praktikum/";
		System.out.println("Creating target directory: " + targetDir);
		try {
			fs.mkdirs(new Path(targetDir));
		} catch (IOException e1) {
			System.out.println("Could not create target directory.");
			e1.printStackTrace();
		}
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
