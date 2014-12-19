/*
 * ColStoreTest.java
 * derived from DataPopulator.java
 * json data is from a file, each line is a json object
 * @author Jin Chen
 */

//package cheetah;

import java.io.FileReader;
import java.io.FileInputStream;
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


public class ColStoreTest {
    public static void main(String[] args){

        String inputFileName; 
        int buffer_size = 100*1000*1000;
        //System.out.println("Buffer size (inGB) "+buffer_size/1000/1000/1000); 
        ColStore dataStore = new ColStore(buffer_size); 

        if (args.length != 2){
            System.out.println("Arguments: "+ "<input file name> <test column name>" );
            System.exit(0);
        }

        try{
            inputFileName = args[0];
            /* if the input file is JSON array -- use this part

            //this is an JSON array containing a lot of JSON objects
            /*
            FileInputStream is = new FileInputStream(inputFileName);
            JsonReader reader = Json.createReader(is);
            JsonArray array = reader.readArray();
            for (int i = 0; i< array.size() ;i++){
                JsonObject ob = array.getJsonObject(i);
                dataStore.insertObject(i,ob,null);
            }
            System.out.println("array size"+array.size());
            */
            

            /*
            BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFileName));
            String line = bufferedReader.readLine();
            int objid = 0;
            while (line !=null){
                objid  = objid + 1;
                InputStream is = new ByteArrayInputStream(line.getBytes());
                JsonReader reader = Json.createReader(is);
                JsonObject jsonob = reader.readObject();
                //System.out.println(jsonob.toString());
                dataStore.insertObject(objid,jsonob,null);
                line = bufferedReader.readLine();
            }
            */

            /* output the row - for test*/
            //dataStore.getObject(3);
            //use last column as the where column
            String column = args[1];
            int numTests = 1;
            for(int i=0; i<numTests; i++){
                long start = System.currentTimeMillis();
                long sum=dataStore.aggregate(column.getBytes(),10); 
                //System.out.println("sum is "+sum);
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

