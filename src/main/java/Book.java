import java.io.Serializable;

public class Book implements Serializable {
String bookName;
String bookAuthor;
	
public String getBookName() {
	return bookName;
}
public void setBookName(String bookName) {
	this.bookName = bookName;
}
public String getBookAuthor() {
	return bookAuthor;
}
public void setBookAuthor(String bookAuthor) {
	this.bookAuthor = bookAuthor;
}
public Book(String bookName,String bookAuthor)
{
	this.bookName = bookName;
	this.bookAuthor = bookAuthor;
}

}
