/*
 * ColStoreTestSelect.java
 * derived from DataPopulator.java
 * json data is from a file, each line is a json object
 * @author Alan Lu
 */

//package cheetah;

import java.io.FileReader;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import java.nio.ByteBuffer;
import java.util.Calendar;


public class ColStoreSelectWhereTest {
    public static void main(String[] args){

        String inputFileName; 
        int buffer_size = 100*1000*1000;

        if (args.length != 4){
            System.out.println("Arguments: "+ "<input file name> <select columns separated by ：> <where column name> <threshold>" );
            System.exit(0);
        }
	
		ColStore colStore = new ColStore(buffer_size);
        
		try{
            inputFileName = args[0];
            BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFileName));
            String line = bufferedReader.readLine();
            int objid = 0;
            while (line !=null){
                objid  = objid + 1;
                InputStream is = new ByteArrayInputStream(line.getBytes());
                JsonReader reader = Json.createReader(is);
                JsonObject jsonob = reader.readObject();
                //System.out.println(jsonob.toString());
				colStore.insertObject(objid,jsonob,null);
                line = bufferedReader.readLine();
            }
			String[] selectColumns = args[1].split(":");
			byte[][] columns = new byte[selectColumns.length][];
			for(int i=0; i<selectColumns.length; i++){
				String column = selectColumns[i];
				columns[i] = column.getBytes();
			}
            int numTests = 1;
            for(int i=0; i<numTests; i++){
                long start = System.currentTimeMillis();
                long sum = colStore.selectWhere(columns, args[2].getBytes(), Integer.parseInt(args[3])); 
                long end = System.currentTimeMillis();
                System.out.print(sum+" ");
				System.out.println(end-start);

            }
        } catch (FileNotFoundException e){
            System.err.println("FileNotFoundException:"+e.getMessage());
            return ;
        } catch (IOException e){
            System.err.println("IOException:"+e.getMessage());
            return ;
        }
    }
}

