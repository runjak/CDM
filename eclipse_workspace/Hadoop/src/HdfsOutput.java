/**
 * 
 */
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
