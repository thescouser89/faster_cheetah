
/**
 * RowColStore
 *
 * Assumption: we don't have synchronization so far   
 *             so no need to protect about buffer array acess 
 *
 * @author Alan Lu
 */
import java.io.FileReader;
import java.io.BufferedReader;
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
import java.util.Hashtable;
import java.util.Arrays;



public class RowColStore {

	private int max_buf_size;
	private int  UNDEFINED= -1111111; //represent NULL  
    private byte [] UndefByte= new byte[1];
	private static String [][] fields;
    /* key as the key for the hash table */
    Hashtable<String, String> hybridBufs; /* multiple keys could direct to the same buffer */
    Hashtable<String, ByteBuffer> searchBufs; /* one key one buffer - for get the object out */
	Hashtable<String, Long> tempBufLong;
    Hashtable<String, String> types;
	
	/**
         * Creates a ring buffer, and using Column format to store the data
	 * initialize the buffer size 
	 */
	public RowColStore (int memory_size_in_bytes,String layoutFile)
	{
		/* later -- pass it from paramters */
		max_buf_size = memory_size_in_bytes; /* allocated first */

        /* create colum Buffer hash table */ 
        hybridBufs = new Hashtable<String, String>();
        searchBufs = new Hashtable<String, ByteBuffer>();
        tempBufLong = new Hashtable<String, Long>();
        types = new Hashtable<String, String>();

        UndefByte[0] = -1;

        /* create buffer first */
        /* read layout file, create each buffer for each line */
        /* key is field_name:type, value is the buf */

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(layoutFile));
            String line  = bufferedReader.readLine();
            int i = 0;
            while(line !=null){
                String[] keys = line.split("\\s+"); //separted by any white space 
                if(keys.length == 0)  
                    continue; // empty line
                ByteBuffer buf = ByteBuffer.allocateDirect(max_buf_size);
                //System.out.println("allocate buf "+max_buf_size +" for " + keys);
                for (String k : keys){
                    hybridBufs.put(k,line);
                }
                searchBufs.put(line,buf);
                i++;
                line = bufferedReader.readLine();
            }
        } catch (FileNotFoundException e){
			System.err.println("FileNotFoundException:"+e.getMessage());
			return ;
        } catch (IOException e){
			System.err.println("IOException:"+e.getMessage());
			return;
        }
	}

	/* Return the sum of specified columns*/
	public long select(byte[][] columns){	
		long sum = 0;
		for(String skey: searchBufs.keySet()){
			//String skey = "1";
	
     	//System.out.println(skey);
		    String[] keys = skey.split("\\s+"); //separted by any white space
        	int[] selectFields = new int[keys.length];
			boolean containColumn = false;

			for(int i = 0; i < keys.length; i++){
				selectFields[i] = 0;
				String columnKey = keys[i];
				for(int j = 0; j < columns.length; j++){
	 				if(Arrays.equals(columnKey.getBytes(), columns[j])==true){
						selectFields[i] = 1;
						containColumn = true; 
					}
				}
			}

			if(containColumn == false){
				continue;
			}

		    ByteBuffer buf = searchBufs.get(skey);
			int bound = buf.position();
            ByteBuffer readBuf = buf.asReadOnlyBuffer();
            readBuf.position(0);
            long longnum = 0;
           	
         	while(readBuf.position()<bound){
				readBuf.position((readBuf.position() + 4));
				for(int i = 0 ; i < keys.length; i++){
					//longnum = readBuf.getLong();
					if(selectFields[i] == 1){
						longnum = readBuf.getLong();
						sum += longnum;
					}
					else{
						int oldpos = readBuf.position();
						int newpos = oldpos + 8; //one long
						readBuf.position(newpos);
					}
				}
			}
		}
		return sum;
	}
    
    /* Return the sum of specified columns if condition in where is met
     *  Assuming there's only one where column and there can be multiple select columns
     */
	public long selectWhere(byte[][] selectCols,  byte[] whereCol, int threshold){
		long sum = 0;
		long longnum = 0;
        ByteBuffer [] selectBufs = new ByteBuffer [searchBufs.size()];
        int [] bounds = new int [searchBufs.size()];
        int [][] Fields = new int [searchBufs.size()][];
        int [] positions = new int [searchBufs.size()];
        
		//index starts at 1 because index=0 is for thr bytebuffer with where field
        int index = 1;
        int whereIndex = 0;
        for(String skey: searchBufs.keySet()){
            String[] keys = skey.split("\\s+");
			int[] fields = new int[keys.length];
			boolean containSelect = false;
			boolean containWhere = false;
			for(int i = 0; i < keys.length; i++){
                fields[i] = 0;
                String columnKey = keys[i];
				if(Arrays.equals(columnKey.getBytes(), whereCol)==true){
					fields[i] = 2;
                    whereIndex = 0;
					containWhere = true;
				}
				for(int j = 0; j < selectCols.length; j++){
					if(Arrays.equals(columnKey.getBytes(), selectCols[j])==true){
						fields[i] = 1;
						containSelect = true;
					}
				}
            }
			ByteBuffer sbuf = searchBufs.get(skey);
			ByteBuffer rbuf = sbuf.asReadOnlyBuffer();
			rbuf.position(0);
            if(containWhere == true){
                // this bytebuffer contains the where field
                bounds[0] = sbuf.position();
                selectBufs[0] = rbuf;
                positions[0] = 0;
				Fields[0] = fields;
				continue;
			}
			if(containSelect==true){
				//this bytebuffer contains select fields
                bounds[index] = sbuf.position();
                selectBufs[index] = rbuf;
                positions[index] = 0;
				Fields[index] = fields;
                index += 1;
			}
        }
        
		if(selectBufs[0] == null){
			System.out.println("Didn't find buffer that contains the where field");
			return -1;
		}
        ByteBuffer readBuf = selectBufs[0];
		int wbound = bounds[0];
		int wposition = positions[0];
		int[] wfields = Fields[0];
        int rowSkipped = 0;

        //we only need to scan one bytebuffer, which include everything we need
		if(index <= 1){
            while(readBuf.position()<wbound){
                wposition += 4; //skip the objid
                //skip to the wherefield
                readBuf.position(wposition+whereIndex*8);
                longnum = readBuf.getLong();
                if(longnum >= threshold){
                    wposition +=  8*(wfields.length - whereIndex);
                    continue;
                }
                
                for(int i = 0; i < wfields.length; i++){
                    if(wfields[i] == 1){
                        readBuf.position(wposition);
                        longnum = readBuf.getLong();
                        sum += longnum;
                    }
                    wposition += 8;
                }
            }
		}
        else{
			while(readBuf.position()<wbound){
            	wposition += 4; //skip the objid
            	//skip to the wherefield
	            readBuf.position(wposition+whereIndex*8);
    	        longnum = readBuf.getLong();
                if(longnum >= threshold){
                    wposition +=  8*(wfields.length - whereIndex);
                    rowSkipped++;
                    continue;
                }
                //readBuf.position(wposition);
                for(int i = 0; i < wfields.length; i++){
                    if(wfields[i] == 1){
                        readBuf.position(wposition);
                        longnum = readBuf.getLong();
                        sum += longnum;
                    }
                    wposition += 8;
                }
                //go through other bytebuffer
                for(int i = 1; i < index; i++){
                    positions[i] += ((4 + 8*Fields[i].length)*rowSkipped + 4);
                    for(int j = 0; j < Fields[i].length; j++){
                        if(Fields[i][j] == 1){
                            selectBufs[i].position(positions[i]);
                            longnum = selectBufs[i].getLong();
                            sum += longnum;
                        }
                        positions[i] += 8;
                    }
                }
                rowSkipped = 0;
    	    }
        }
		return sum;
	}
   
	public int getObject(int targetId)
	{
        /* tranverse each buffer to read it */
        //get the buffer
        for(String skey: searchBufs.keySet()){
            String[] keys = skey.split("\\s+"); //separted by any white space
			// check the key and assign different method
            ByteBuffer buf = searchBufs.get(skey);
            int bound = buf.position();
            ByteBuffer readBuf = buf.asReadOnlyBuffer();
            readBuf.position(0);
            int oid = 0;
            long longnum = 0;
            
            while(readBuf.position() < bound) {
                oid = readBuf.getInt();
                //System.out.print("current cursor: Rowid "+oid+" key "+skey);
                if(oid > targetId)
                    break; // already found all of the target row , assume monotonous increase
                if(oid == targetId)
                    System.out.print("Row "+oid);
                
                for(int i=0; i<keys.length; i++){
                    String type = types.get(keys[i]);
                    if(type.equals("STRING")){
                    }
                    else if(type.equals("LONG")){
                        longnum = readBuf.getLong();
                        if(oid == targetId){
                            System.out.print(longnum + " ");
                        }
                    }
                    else if(type.equals("DOUBLE")){
                    }
                    else if(type.equals("BOOL")){
                    }
                }
                if(oid == targetId)
                    System.out.println("");
            } //end while
        } // end for
        return 0;
    }

	public void insertRow(int objid){
        
		for(String skey: searchBufs.keySet()){
            String[] keys = skey.split("\\s+"); //separted by any white space
            ByteBuffer buf = searchBufs.get(skey);
            
			//leave 1K as the warning threshold - don't insert after it 
	    	if(buf.position() > buf.capacity() - 1000){
	    		//don't insert any more -- buffer almost full
	    		System.out.println("buffer is almost full. No inserts any more!");
	    		break;
	    	}

			buf.putInt(objid);
            for(int i=0; i<keys.length; i++){
                String type = types.get(keys[i]);
                if(type.equals("STRING")){
                }
                else if(type.equals("LONG")){
                    if(tempBufLong.get(keys[i]) != null)
                        buf.putLong(tempBufLong.get(keys[i]));
                    else
                        buf.putLong(UNDEFINED);
                }
                else if(type.equals("DOUBLE")){
                }
                else if(type.equals("BOOL")){
                }
                else{
                    System.out.println("Error: no such type in buf "+type);
                    return;
                }
            }
        }
    }

	public void insertObject(int objid, JsonValue tree, String key){
		tempBufLong.clear();
		prepareRow(objid, tree, key);
		insertRow(objid);
	}

	public void prepareRow(int objid, JsonValue tree, String key){
		switch(tree.getValueType()){
			case OBJECT:
				//System.out.println("  OBJECT");
				JsonObject object = (JsonObject) tree;
				for(String name: object.keySet()){
					if(key!=null)
						prepareRow(objid,object.get(name),key+"."+name);
					else
						prepareRow(objid,object.get(name),name);
				}
                //if((objid % 10000) == 1) 
                //   System.out.println("Row id " + objid+ "buffer offset "+buffer.position());
				break;
			case ARRAY:
				JsonArray array = (JsonArray) tree;
				int index =0; 
				for (JsonValue val : array){
					prepareRow(objid,val,key+"["+index+"]");
					index += 1;
				}
				break;
			case STRING:
				break;
			case NUMBER:
				JsonNumber num = (JsonNumber) tree;
                if(num.isIntegral()){
                    types.put(key, "LONG");
					tempBufLong.put(key, num.longValue());
                }else{
                }
				break;
			case TRUE:
			case FALSE:
				break;
			case NULL:
				break;
		}
	}


	public static void main(String[] args) throws IOException{

		/* flatten the json file */ 
		JsonReader reader = Json.createReader(new FileReader("testjson/abcde.json")); 
		JsonObject jsonob = reader.readObject();
		System.out.println(jsonob.toString());
		RowColStore parser= new RowColStore(10*1000*1000,"testjson/abcde.layout");
		int objid = 1;
		parser.insertObject(objid,jsonob,null);
		parser.insertObject(2,jsonob,null);
		parser.insertObject(3,jsonob,null);
		// populate the table 

		System.out.println("get the result out \n");
		// objid, keystr,valstr,valnum,valbool - 5 bytes 
		// read it out 
	//	parser.getObject(2);
	}
}
