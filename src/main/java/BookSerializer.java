import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class BookSerializer implements Serializer<Book>
{
	private String encoding="UTF8";
	
	@Override
	public void configure(Map<String,?> configs, boolean iskey) {/*Nothing*/ }

	@Override
	public byte[] serialize (String topic, Book book)
	{
		int sizeOfBookName;
		int sizeOfBookAuthor;
		byte[] serializedBookName;
		byte[] serializedBookAuthor;
		
		try
		{
			if(book ==null)
				return null;
			serializedBookName=book.getBookName().getBytes(encoding);
			sizeOfBookName=serializedBookName.length;
			serializedBookAuthor=book.getBookAuthor().getBytes(encoding);
			sizeOfBookAuthor=serializedBookAuthor.length;
				
			ByteBuffer buffer =ByteBuffer.allocate(4+sizeOfBookName+4+sizeOfBookAuthor);
			
			buffer.putInt(sizeOfBookName);
			buffer.put(serializedBookName);
			buffer.putInt(sizeOfBookAuthor);
			buffer.put(serializedBookAuthor);
			
			return buffer.array();
		}
		catch(Exception e)
		{
			throw new SerializationException("serializing book to byte");
		}
	}
	
@Override
public void close()
{
	/*nothing*/

}
}

