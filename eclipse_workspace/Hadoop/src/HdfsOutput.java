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
 *gibt die HDFS-Datei /input_data/praktikum/movies.dat auf der Standardausgabe aus
 */
public class HdfsOutput {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		FileSystem fs = null;
		String fsUri = "hdfs://localhost:8019";
		
		//Pfad zur Datei im HDFS
		String path = "/input_data/praktikum/movies.dat";
		Path p = new Path(fsUri + path);
		
		try {
		//zum Filesystem verbinden (HDFS muss laufen!!)	
		fs = FileSystem.get(new URI(fsUri), new Configuration());
		
		//Dateigroesse feststellen
		FileStatus fStat = fs.getFileStatus(p);
		int size = (int)fStat.getLen();
		
		//Datenstrom oeffnen und Datei in Buffer lesen, max Groesse: maxInt Byte
		FSDataInputStream in = new FSDataInputStream(fs.open(p));
		byte[] buffer = new byte[size];
		in.readFully(buffer);
		
		//Ausgabe auf System
		System.out.println(new String(buffer));
		} catch (Exception e){
			System.out.println("Exception! ");
			e.printStackTrace();
		}
	}

}
