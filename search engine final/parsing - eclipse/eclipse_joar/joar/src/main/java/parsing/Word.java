package parsing;
/**
* The Word program implements the initialization of name and idword
* variables and the methods to set and return these variables.
*
* @author  J0arTeam
* @version 1.0
* @since   2017-01-03
*/
public class Word{
	/** Η μεταβλητή με το όνομα της λέξης σε μορφή String*/
	private String name;
	/** Η μεταβλητή με τον αύξων αριθμό των λέξεων στον πίνακα Word της βάσης */
	private int idword;

    /** Constructor to initiliaze the id_word and name variables*/
	public Word(int idword, String name){
		this.idword=idword;
		this.name=name;
	}

	/** Constructor to initiliaze only the name variable*/
	public Word(String name){
			this.name=name;
	}

    /**
	 *
	 * @param name name to set
     */
	public void setName(String name){
		this.name = name;
	}
	/**
	 * @return current name
    */
	public String getName(){
		return name;
	}
  /**
  *
  * @param idword idword to set
  */
	public void setId(int idword){
		this.idword = idword;
	}
	/**
	 * @return current idword
     */
	public int getId(){
		return idword;
	}
}
