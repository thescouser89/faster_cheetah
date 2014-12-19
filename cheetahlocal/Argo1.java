
/**
 * An row buffer store using Argo1 representation 
 * @author Jin Chen
 */
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Arrays;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import java.nio.ByteBuffer;


public class Argo1 {

	private int max_buf_size;
	private ByteBuffer buffer; //save all of the contents
	private int  UNDEFINED= -1111111; //represent NULL  
    private byte [] UndefByte= new byte[1];


	/**
     * Creates a ring buffer, and using Argo1 format to store the data
	 * initialize the buffer size 
     */
    public Argo1 (int memory_size_in_bytes)
    {
        
		int max_buf_size = memory_size_in_bytes; 

		// create a byte buffer 
		buffer = ByteBuffer.allocateDirect(max_buf_size);
		//buffer = new byte[max_buf_size];

        UndefByte[0] = -1;

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
		//later -- use dictionary to compress
		buffer.putInt(objid);
		buffer.putInt(key.length());
		buffer.put(key.getBytes());
		buffer.putInt(value.length());
		buffer.put(value.getBytes());
		buffer.putLong(UNDEFINED);
		buffer.put(UndefByte);
		return 0;
		
	}

	private int saveLongValue(int objid, String key, long num)
	{
		buffer.putInt(objid);
		buffer.putInt(key.length());
		buffer.put(key.getBytes());
		buffer.putInt(UNDEFINED);
		buffer.putLong(num);
		buffer.put(UndefByte);
		return 0;
	} 

	private int saveDoubleValue(int objid, String key, double num)
	{
		buffer.putInt(objid);
		buffer.putInt(key.length());
		buffer.put(key.getBytes());
		buffer.putInt(UNDEFINED);
		buffer.putDouble(num);
		buffer.put(UndefByte);
		return 0;
	} 

	private int saveBoolValue(int objid, String key, String value)
	{
		buffer.putInt(objid);
		buffer.putInt(key.length());
		buffer.put(key.getBytes());
		buffer.putInt(UNDEFINED);
		buffer.putLong(UNDEFINED);
		if(value.equals("TRUE")==true){
			buffer.put((byte)1);
		}else if(value.equals("FALSE")==true){
			buffer.put((byte)0);

		}else{
			System.out.println("Error: unknow value "+value);
		}
		return 0;
	}

	/**
	 * Fixed row format [objid,keystr,valstr,valnum,valbool] 
	 */

	public int getRow(int targetId)
	{
		ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        // start from the beginning 
		readBuf.position(0);
		int oid = 0;
		byte [] key,valstr;
		long valnum=0;
		int len;
        byte []valbool=new byte[1];
		while(readBuf.position()<bound){ 
            // scan the buffer 
			oid = readBuf.getInt();
			if(oid > targetId)
				break; /* found the target row - assume monotonous increase */
			if(oid == targetId)
				System.out.print("Row "+oid);
			len = readBuf.getInt();
			key = new byte [len];
			readBuf.get(key);
			if(oid == targetId)
				System.out.print("  "+new String(key));
			len = readBuf.getInt();
			if(len != UNDEFINED){
				valstr = new byte [len];
				readBuf.get(valstr);
				if(oid == targetId)
					System.out.print("  "+new String(valstr));
					//System.out.print("  "+valstr);
				/* skip other two fields */
				readBuf.getLong();
				readBuf.get(valbool);
			}else{
				valnum = readBuf.getLong();
				
				if(valnum == UNDEFINED){
					readBuf.get(valbool);	
					if(valbool[0] != 0 && valbool[0] != 1){
						System.out.println("wrong bool value!"+valbool);
						System.exit(0);		
					}
					if(oid == targetId)
						System.out.print(" "+valbool[0]);
				}else{
					if(oid == targetId)
						System.out.print(" "+valnum);
					/* skip last bool filed */
					readBuf.get(valbool);
				}
			}
			if(oid == targetId)
				System.out.println("");
		} /* end while */
        return 0;
	}

	/**
	* Navigate the json object and parse it and save it into storage 
	*
	*/

	public void insertRow(int objid, JsonValue tree, String key){
        //leave 1K as the warning threshold - don't insert after it 
        if(buffer.position() > buffer.capacity() - 1000){
            //don't insert any more -- buffer almost full
            System.out.println("buffer is almost full. No inserts any more!");
            return;
        }
		switch(tree.getValueType()){
			case OBJECT:
				//System.out.println("  OBJECT");
				JsonObject object = (JsonObject) tree;
				for(String name: object.keySet()){
					if(key!=null)
						insertRow(objid,object.get(name),key+"."+name);
					else
						insertRow(objid,object.get(name),name);
				}
                // check the size
                //if((objid % 10000) == 1) 
                //   System.out.println("Row id " + objid+ "buffer offset "+buffer.position());
				break;
			case ARRAY:
				//System.out.println("  ARRAY");
				JsonArray array = (JsonArray) tree;
				int index =0; 
				for (JsonValue val : array){
					insertRow(objid,val,key+"["+index+"]");
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

	/**
	* execute a simple aggregation query with one condition check     
	* first, just do scan, then do sum up
	*/
    public long aggregate(byte[] colName,int threshold){
		ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        // start from the beginning 
		readBuf.position(0);
		byte [] key,valstr;
		long valnum=0;
		int len;
        byte []valbool=new byte[1];
        long sum=0;
        long rowsum = 0;
        int preOid = -1; //invalid value, need to be assigned in the first row access  
		int oid = 1;
        boolean selectFlag = false; //indicate whether this row is selected or not, based on the condition
        boolean fieldFlag = false; //indicate whether this field is the conditional check field or not 
		while(readBuf.position()<bound){ 
            // scan the buffer 
			oid = readBuf.getInt();
            //System.out.println("scan oid "+oid);

            if(preOid == -1){
                preOid = oid; //initialize 
			}else if(oid > preOid){
				// found the next object - assume monotonous increase 
                if(selectFlag==true){
                    sum += rowsum;
                    //System.out.println("row sum value "+rowsum+" sum "+sum);
                }
                //System.out.println("reset rowsum,selectFlag");
                // reset row stats and selectFlag -- must happen before we check the key
                rowsum = 0;
                selectFlag = false;
                preOid = oid;
            }

			len = readBuf.getInt();
			key = new byte [len];
			readBuf.get(key);

            //compare the key -- assume different columns in buffer have no order -- have to check the key 
            //this is reasonable, become sometime, some columns are missing
            if(Arrays.equals(key,colName)==true){
                fieldFlag = true;
            }else{
                fieldFlag = false;
            }
            
			len = readBuf.getInt();
			if(len != UNDEFINED){
				valstr = new byte [len];
				readBuf.get(valstr);
				/* skip other two fields */
				readBuf.getLong();
				readBuf.get(valbool);
			}else{
				valnum = readBuf.getLong();
				
				if(valnum == UNDEFINED){
					readBuf.get(valbool);	
					if(valbool[0] != 0 && valbool[0] != 1){
						System.out.println("wrong bool value!"+valbool);
						System.exit(0);		
					}
				}else{
                    //LONG value 
                    //check whether it is where field  
                    if(fieldFlag==true){
                        //check selectivity 
                        if(valnum <= threshold){
                            selectFlag = true; 
                        } 
                    }else{
                        //sum up if it is not where field
                        rowsum += valnum;
                        //System.out.println("row sum value "+rowsum+" val "+valnum+" sum "+sum);
                    }

					/* skip last bool filed */
					readBuf.get(valbool);
				}
			}
		} // end while 

        //System.out.println("End loop: row sum value "+rowsum+" val "+valnum+" sum "+sum+" selectFlag "+selectFlag);
        // check last row 
        if(selectFlag==true){
            sum += rowsum;
        }
        return sum;

    }

	public static void main(String[] args) throws IOException{

		//JsonReader reader = Json.createReader(new FileReader("testjson/test.json")); 
		JsonReader reader = Json.createReader(new FileReader("testjson/abc.json")); 
		JsonObject jsonob = reader.readObject();
		System.out.println(jsonob.toString());

		Argo1 store= new Argo1(100*1000*1000);
		int objid = 1;
		store.insertRow(objid,jsonob,null);
		store.insertRow(2,jsonob,null);
		store.insertRow(3,jsonob,null);

		System.out.println("get the result out \n");
		/* objid,keystr,valstr,valnum,valbool - 5 bytes */

		/* read it out */
		store.getRow(1);

        // aggregate scan   
        String targetColumn = "B";
        long sum = store.aggregate(targetColumn.getBytes(),10);
        System.out.println("Aggregate sum Results : "+sum);

	}
}
