
/**
 * HybridStore
 * Save JSON file into memory based hybrid store 
 * Colocated JSON fields are saved together in a buffer
 * Now we use an indicator file to decide which fields are saved together 
 * 
 * We save objid + key + value for each key, later we can optimize to not save key info into data, but into schema
 * We ignore dynamic type for now. Use key itself as the key to retrieve the buffer
 * current saving method is like Argo1
 *
 * Assumption: we don't have synchronization so far   
 *             so no need to protect about buffer array acess 
 *
 * @author Jin Chen
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



public class HybridStore {

	private int max_buf_size;

	private int  UNDEFINED= -1111111; //represent NULL  
    private byte [] UndefByte= new byte[1];

    /* key as the key for the hash table */
    Hashtable<String, ByteBuffer> hybridBufs; /* multiple keys could direct to the same buffer */
    Hashtable<String, ByteBuffer> searchBufs; /* one key one buffer - for get the object out */



	/**
         * Creates a ring buffer, and using Column format to store the data
	 * initialize the buffer size 
	 */
	public HybridStore (int memory_size_in_bytes,String layoutFile)
	{
		/* later -- pass it from paramters */
		max_buf_size = memory_size_in_bytes; /* allocated first */

        /* create colum Buffer hash table */ 
        hybridBufs = new Hashtable<String, ByteBuffer>();  
        searchBufs = new Hashtable<String, ByteBuffer>();  
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
                System.out.println("allocate buf "+max_buf_size +" for " + keys);
                for (String k : keys){
                    hybridBufs.put(k,buf);
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

	/**
	 * not Fixed row format [objid,keystr,valstr,valnum,valbool] 
     * objid - int, 4Bytes
     * keystr = int + chars, 4Bytes + length
     * valstr = int + chars, 4 Bytes + length
     * valnum -- using long or double (64bits, 8 bytes) - later distinguish them 
     *           8 Bytes  
	 * valbool = 1 byte 
	 */

	private int saveStrValue(int objid, String key, String value)
	{
        /* check whether we have buffer or not */

		//later -- use dictionary to compress
        // save to str
        //System.out.println(key);
        ByteBuffer buf=hybridBufs.get(key);
        if(buf == null){
            System.out.println("no pre-allocated buffer! please define "+key+" into the layout file! ");
            return -1;
        }
        // insert  
	    buf.putInt(objid);
        //System.out.println("Insert "+key +" to object id "+objid); 
	    buf.putInt(key.length());
        buf.put(key.getBytes());
		buf.putInt(value.length());
		buf.put(value.getBytes());
        buf.putLong(UNDEFINED);
        buf.put(UndefByte);
        
		return 0;
		
	}

	private int saveLongValue(int objid, String key, long num)
	{
        ByteBuffer buf=hybridBufs.get(key);
        if(buf == null){
            //allocate byte buffer
            System.out.println("no pre-allocated buffer! please define "+key+" into the layout file! ");
            return -1;
        }

        buf.putInt(objid);
	    buf.putInt(key.length());
        buf.put(key.getBytes());
        buf.putInt(UNDEFINED);
        buf.putLong(num);
        buf.put(UndefByte);

		return 0;
	} 

	private int saveDoubleValue(int objid, String key, double num)
	{
        ByteBuffer buf=hybridBufs.get(key);
        if(buf == null){
            //allocate byte buffer
            System.out.println("no pre-allocated buffer! please define "+key+" into the layout file! ");
            return -1;
        }

        buf.putInt(objid);
	    buf.putInt(key.length());
        buf.put(key.getBytes());
        buf.putInt(UNDEFINED);
        buf.putDouble(num);
        buf.put(UndefByte);

		return 0;
	} 

	private int saveBoolValue(int objid, String key, String value)
	{
        ByteBuffer buf = hybridBufs.get(key);
        if(buf == null){
            //allocate byte buffer
            System.out.println("no pre-allocated buffer! please define "+key+" into the layout file! ");
            return -1;
        }

        buf.putInt(objid);
	    buf.putInt(key.length());
        buf.put(key.getBytes());
        buf.putInt(UNDEFINED);
        buf.putLong(UNDEFINED);
		if(value.equals("TRUE")==true){
		    buf.put((byte)1);
		}else if(value.equals("FALSE")==true){
		    buf.put((byte)0);
		}else{
		    System.out.println("Error: unknow value "+value);
		}
		return 0;
	}

	/**
	 * Fixed row format [objid,keystr,valstr,valnum,valbool] 
	 */

	public int getObject(int targetId)
	{
        /* tranverse each buffer to read it */
        //get the buffer
        System.out.println("Iterating on the buffer hashtable"+searchBufs.keySet());
        for(String skey: searchBufs.keySet()){
            // check the key and assign different method
            ByteBuffer buf = searchBufs.get(skey);
            int bound = buf.position();
            ByteBuffer readBuf = buf.asReadOnlyBuffer();
            readBuf.position(0);
            int oid = 0;
            byte [] key,valstr;
            long valnum = 0;
            int len;
            byte [] valbool = new byte[1];

            while(readBuf.position() < bound) {
                oid = readBuf.getInt();
                //System.out.print("current cursor: Rowid "+oid+" key "+skey);
                if(oid > targetId)
                    break; // already found all of the target row , assume monotonous increase
                if(oid == targetId)
                    System.out.print("Row "+oid);
                len = readBuf.getInt();
                key = new byte[len];
                readBuf.get(key);
                if(oid == targetId)
                    System.out.print(" "+ new String(key));
                len = readBuf.getInt();
                if(len != UNDEFINED){
                    //STRING type
                    valstr = new byte[len];
                    readBuf.get(valstr);
                    if(oid == targetId)
                        System.out.print(" "+new String(valstr));
                    //skip other two fields 
                    readBuf.getLong();
                    readBuf.get(valbool);
                }else{
                    valnum = readBuf.getLong();
                    if(valnum == UNDEFINED){
                        //BOOL type
                        readBuf.get(valbool);
                        if(valbool[0] != 0 && valbool[0] != 1){
                            System.out.println("wrong bool value!"+valbool);
                            System.exit(0);
                        }
                        if(oid == targetId)
                            System.out.print(" " + valnum);
                    }else{
                        //NUMBER type - DOUBLE or LONG
                        if(oid == targetId)
                            System.out.print(" " + valnum);
                        // skip last bool filed             
                        readBuf.get(valbool);
                    }
                }
                if(oid == targetId)
                    System.out.println("");
            } //end while
        } // end for
        return 0;
    }


    /**
    *
    Table 1: objid + keystr (4B + 4B + length)
    Table 2: objid + valstr (4B + 4B + length)
    Table 3: objid + INT   (4B+4B - int array)
    Talbe 4: objid + LONG (4B + 8B
    Table 5: objid + DOUBLE
    Table 6: objid + BOOL ( 4B + 1B )  -- maybe compressed with objid together
    *
    */

	/**
	* Navigate the json object and parse it and save it into storage 
	*
	*/

	public void insertObject(int objid, JsonValue tree, String key){

		switch(tree.getValueType()){
			case OBJECT:
				//System.out.println("  OBJECT");
				JsonObject object = (JsonObject) tree;
				for(String name: object.keySet()){
					if(key!=null)
						insertObject(objid,object.get(name),key+"."+name);
					else
						insertObject(objid,object.get(name),name);
				}
                //if((objid % 10000) == 1) 
                //   System.out.println("Row id " + objid+ "buffer offset "+buffer.position());
				break;
			case ARRAY:
				//System.out.println("  ARRAY");
				JsonArray array = (JsonArray) tree;
				int index =0; 
				for (JsonValue val : array){
					insertObject(objid,val,key+"["+index+"]");
					index += 1;
				}
				break;
			case STRING:
				JsonString st = (JsonString) tree;
				saveStrValue(objid,key,st.getString());
				//System.out.println(objid+" "+key+" "+st.getString());
				break;
			case NUMBER:
				JsonNumber num = (JsonNumber) tree;
                if(num.isIntegral()){
				    saveLongValue(objid,key,num.longValue());
                }else{
                    saveDoubleValue(objid,key,num.doubleValue());
                }
				//System.out.println(objid+" "+key+" "+num.toString());
				break;
			case TRUE:
			case FALSE:
				saveBoolValue(objid,key,tree.getValueType().toString());
				//System.out.println(objid+" "+key+" "+tree.getValueType().toString());
				break;
			case NULL:
                // we didn't save null value
				//System.out.println("null\n:");
				//System.out.println(objid+"key "+key+tree.getValueType().toString());
				break;
		}


	}

	public static void main(String[] args) throws IOException{


		/* flatten the json file */ 
		JsonReader reader = Json.createReader(new FileReader("testjson/abcde.json")); 
		JsonObject jsonob = reader.readObject();
		System.out.println(jsonob.toString());
		HybridStore parser= new HybridStore(10*1000*1000,"testjson/abcde.layout");
		int objid = 1;
		parser.insertObject(objid,jsonob,null);
		parser.insertObject(2,jsonob,null);
		parser.insertObject(3,jsonob,null);
		// populate the table 

		System.out.println("get the result out \n");
		// objid, keystr,valstr,valnum,valbool - 5 bytes 
		// read it out 
		parser.getObject(2);

	}
}
