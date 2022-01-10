import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class BookDeserializer implements Deserializer<Book>
{
private String encoding ="UTF8";
@Override

public void configure(Map<String, ?> configs,boolean iskey)
{
	//nothing 
}
@Override
public Book deserialize(String topic, byte[] data )
{
	try
	{
		
		if(data==null)
			return null;
	
	
	ByteBuffer buffer=ByteBuffer.wrap(data);
	
	int sizeOfBookName=buffer.getInt();
	byte[] bookNameBytes = new byte[sizeOfBookName];
	buffer.get(bookNameBytes);
	String deserializedName = new  String(bookNameBytes,encoding);
	
	int sizeOfBookAuthor=buffer.getInt();
	byte[]bookAuthorBytes = new byte[sizeOfBookName];
	buffer.get(bookAuthorBytes);
	String deserializedAuthor = new String(bookAuthorBytes,encoding);
	
  
	return new Book(deserializedName, deserializedAuthor);
    
	}
     catch (Exception e)
    {
	throw new SerializationException("error when deserialized byte[]");
    }
}

    @Override
  public void close()
 {
	//nothing
 }
 }



