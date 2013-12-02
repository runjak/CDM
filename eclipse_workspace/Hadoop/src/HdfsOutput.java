/**
 * 
 */
import java.io.Console;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
/**
 * @author Martin John
 *
 */
public class HdfsOutput {

	/**
	 * @param args
	 * gibt eine Datei auf der Standardausgabe aus
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		FileSystem fs = null;
		String fsUri = "hdfs://localhost:8019";
		String path = "/home/hadoop/working_dir/hdfs_data/input_data/praktikum/movies.dat";
		Path p = new Path(path);
		try {
		fs = FileSystem.get(new URI(fsUri), new Configuration());
		FileStatus fStat = fs.getFileStatus(p);
		int size = (int)fStat.getLen();
		FSDataInputStream in = new FSDataInputStream(fs.open(p));
		byte[] buffer = new byte[size];
		in.readFully(buffer);
		System.out.println(buffer);
		} catch (Exception e){
			System.out.println("Exception! ");
			e.printStackTrace();
		}
	}

}
