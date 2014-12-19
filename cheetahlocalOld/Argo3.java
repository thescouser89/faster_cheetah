
/**
 * An Buffer using Argo3 representation 
 * @author Jin Chen
 */
import java.io.FileReader;
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


public class Argo3 {

	private int max_buf_size;
	private int start = 0; // start position of the ring buffer
	private int current = 0; // current position of the ring buffer
    private ByteBuffer valStrBuf; //  objid + keystr + valstr (4B + 4B + length + 4B + length) 
    private ByteBuffer valIntBuf; // objid + INT + keystr (4B + 4B + 4B + length ) 
    private ByteBuffer valLongBuf; //objid + LONG + keystr (4B + 8B + 4B + length )
    private ByteBuffer valDoubleBuf; //objid + Double + keystr (4B + 8B + 4B + length ) 
    private ByteBuffer valBoolBuf; //objid + Bool + keystr (4B + 1B + 4B + length)
	private int  UNDEFINED= -1111111; //represent NULL  
    private byte [] UndefByte= new byte[1];



	/**
         * Creates a ring buffer, and using Argo3 format to store the data
	 * initialize the buffer size 
	 */
	public Argo3 (int memory_size_in_bytes)
	{
		/* later -- pass it from paramters */
		int max_buf_size = memory_size_in_bytes; /* allocated first */

		/* create several buffers */
        /* one for each type */
        /* initialize each buffer - use the same number of bytes for now*/ 
        valStrBuf = ByteBuffer.allocateDirect(max_buf_size);
        valIntBuf = ByteBuffer.allocateDirect(max_buf_size);
        valLongBuf = ByteBuffer.allocateDirect(max_buf_size);
        valDoubleBuf = ByteBuffer.allocateDirect(max_buf_size);
        valBoolBuf = ByteBuffer.allocateDirect(max_buf_size);

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
        // save to str
		valStrBuf.putInt(objid);
		valStrBuf.putInt(key.length());
		valStrBuf.put(key.getBytes());
		valStrBuf.putInt(value.length());
		valStrBuf.put(value.getBytes());
		return 1;
		
	}

	private int saveLongValue(int objid, String key, long num)
	{
		valLongBuf.putInt(objid);
		valLongBuf.putInt(key.length());
		valLongBuf.put(key.getBytes());
		valLongBuf.putLong(num);
		return 1;
	} 

	private int saveDoubleValue(int objid, String key, double num)
	{
		valDoubleBuf.putInt(objid);
		valDoubleBuf.putInt(key.length());
		valDoubleBuf.put(key.getBytes());
		valDoubleBuf.putDouble(num);
		return 1;
	} 

	private int saveBoolValue(int objid, String key, String value)
	{
		valBoolBuf.putInt(objid);
		valBoolBuf.putInt(key.length());
		valBoolBuf.put(key.getBytes());
		if(value.equals("TRUE")==true){
			valBoolBuf.put((byte)1);
		}else if(value.equals("FALSE")==true){
			valBoolBuf.put((byte)0);

		}else{
			System.out.println("Error: unknow value "+value);
		}
		return 1;
	}

	/**
	 * Fixed row format [objid,keystr,valstr,valnum,valbool] 
	 */

	public int getObject(int targetId)
	{
        /* tranverse each buffer to read it */
        System.out.println("Read ValStrBuf. used(Bytes) " + valStrBuf.position() );
        getObjectFromStrBuf(targetId);

        System.out.println("Read ValLongBuffer. "+valLongBuf.position());
        getObjectFromLongBuf(targetId); 
        
        System.out.println("Read ValDoubleBuffer. "+valDoubleBuf.position());
        getObjectFromDoubleBuf(targetId);

        System.out.println("Read ValBoolBuffer. "+valBoolBuf.position());
        getObjectFromBoolBuf(targetId);
        return 0;
	}

    public void getObjectFromBoolBuf(int targetId)
    {
        int bound = valBoolBuf.position();
        ByteBuffer readBuf = valBoolBuf.asReadOnlyBuffer();
        readBuf.position(0);
        int oid = 0;
        byte [] key; 
        int len;

        while(readBuf.position()<bound){
            // read object id
            oid = readBuf.getInt();
            if(oid > targetId)
               break; // we assume the target id increase monotonously
            if(oid == targetId){
                System.out.print("Row "+oid);
            }
            // read key
            len = readBuf.getInt();
            key = new byte [len];
            readBuf.get(key);
            if(oid == targetId)
                System.out.print(" " + new String(key));
            // read bool   
            byte [] valbool = new byte[1];
            readBuf.get(valbool);
            if(oid == targetId)
                System.out.println(" " + valbool[0]);
        }
    }

    public void getObjectFromDoubleBuf(int targetId)
    {
        int bound = valDoubleBuf.position();
        ByteBuffer readBuf = valDoubleBuf.asReadOnlyBuffer();
        readBuf.position(0);
        int oid = 0;
        byte [] key; 
        int len;

        while(readBuf.position()<bound){
            // read object id
            oid = readBuf.getInt();
            if(oid > targetId)
               break; // we assume the target id increase monotonously
            if(oid == targetId){
                System.out.print("Row "+oid);
            }
            // read key
            len = readBuf.getInt();
            key = new byte [len];
            readBuf.get(key);
            if(oid == targetId)
                System.out.print(" " + new String(key));
            // read double  
            double value = readBuf.getDouble();
            if(oid == targetId)
                System.out.println(" " + value);
        }
    }

    public void getObjectFromLongBuf(int targetId)
    {
        int bound = valLongBuf.position();
        ByteBuffer readBuf = valLongBuf.asReadOnlyBuffer();
        readBuf.position(0);
        int oid = 0;
        byte [] key; 
        int len;

        while(readBuf.position()<bound){
            // read object id
            oid = readBuf.getInt();
            if(oid > targetId)
               break; // we assume the target id increase monotonously
            //if(oid == targetId){
                System.out.print("Row "+oid);
            //}
            // read key
            len = readBuf.getInt();
            key = new byte [len];
            readBuf.get(key);
            //if(oid == targetId)
                System.out.print(" " + new String(key));
            // read Long  
            long value = readBuf.getLong();
            if(oid == targetId)
                System.out.println(" " + value);
        }

    }


    public void getObjectFromStrBuf(int targetId)
    {
        int bound = valStrBuf.position();
        ByteBuffer readBuf = valStrBuf.asReadOnlyBuffer();
        readBuf.position(0);
        int oid = 0;
        byte [] key,valstr; 
        int len;

        while(readBuf.position()<bound){
            // read object id
            oid = readBuf.getInt();
            if(oid > targetId)
               break; // we assume the target id increase monotonously
            if(oid == targetId){
                System.out.print("Row "+oid);
            }
            // read key
            len = readBuf.getInt();
            key = new byte [len];
            readBuf.get(key);
            if(oid == targetId)
                System.out.print(" " + new String(key));
            // read val string 
            len = readBuf.getInt();
            valstr = new byte[len];
            readBuf.get(valstr);
            if(oid == targetId)
                System.out.println(" " + new String(valstr));
        }

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
		JsonReader reader = Json.createReader(new FileReader("testjson/test.json")); 
		JsonObject jsonob = reader.readObject();
		System.out.println(jsonob.toString());
		Argo3 parser= new Argo3(10*1000*1000);
		int objid = 1;
		parser.insertObject(objid,jsonob,null);
		/* populate the table */

		System.out.println("get the result out \n");
		/* objid, keystr,valstr,valnum,valbool - 5 bytes */
		/* read it out */
		parser.getObject(objid);

	}
}
