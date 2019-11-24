package BigProfiles;

import java.io.Serializable;

@SuppressWarnings("serial")
public class TableRow implements Serializable {
	
	private String id;
	private int value;
	
	public TableRow() {
	}
	
	public TableRow(String id, int value) {
		this.id = id;
		this.value = value;
	}
	
	public String getId() {
		return this.id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public int getValue() {
		return this.value;
	}
	
	public void setValue(int value) {
		this.value = value;
	}
	

}
