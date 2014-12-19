/*
 * RowStoreTest.java
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


public class RowStoreTest {
    public static void main(String[] args){

        String inputFileName; 
        int buffer_size = 2*1024*1024*1024-4;
        //System.out.println("Buffer size (inGB) "+buffer_size/1000/1000/1000); 
        //RowStore rowStore = new RowStore(buffer_size);
        //ColStore dataStore = new ColStore(buffer_size); 

        if (args.length != 4){
            System.out.println("Arguments: "+ "<input file name> <input column def file name> <select column name> <where column name>" );
            System.exit(0);
        }
	
	RowStore rowStore = new RowStore(buffer_size, args[1]);
        
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
				rowStore.insertObject(objid,jsonob,null);
                line = bufferedReader.readLine();
            }
            /* output the row - for test*/
            //dataStore.getObject(3);
            //use last column as the where column
            String column = args[2];
			String column2 = args[3];
            int numTests = 1;
            for(int i=0; i<numTests; i++){
                long start = System.currentTimeMillis();
                //long sum=rowStore.aggregate(column.getBytes(),10); 
                long sum=rowStore.aggregateColumn(column.getBytes(),column2.getBytes(),10); 
               // System.out.println("sum is "+sum);
                long end = System.currentTimeMillis();
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

