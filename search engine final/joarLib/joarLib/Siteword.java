/**
* The Siteword program implements the initialization of site,word,frequency and keyword
* variables and the methods to set and return these variables.
*
* @author  J0arTeam
* @version 1.0
* @since   2017-01-03
*/
package joarLib;
public class Siteword {
   /** Η μεταβλητή που δέχεται στο site σε url σε μορφή String*/
	private String site;
	/** Η μεταβλητή που δέχεται μια συγκεκριμένη λέξη του συγκεκριμένου url σε μορφή String*/
	private String word;
	/**Η συχνότητα της μεταβλητής word στο συγκεκριμένο url*/
	private int frequency;
	/** Η μεταβλητή που παίρνει την τιμή 0 ή 1, ανάλογα με το αν η λέξη είναι λέξη κλειδί ή όχι, σε μορφή int*/
	private int keyword;
	/** Constructor to initiliaze the site,word,frequency and keyword variables*/
	public Siteword(String site, String word, int frequency) {
		this.site = site;
		this.word = word;
		this.frequency = frequency;
	}

	/** Constructor to initiliaze the site*/
	public Siteword(String site) {
	    this.site = site;
    }

	/**
	 * @return current site
	 */
	public String getSite() {
		return site;
	}

	/**
	 *
	 * @param site site to set
	 */
	public void setSite(String site) {
		this.site = site;
	}

	/**
	 * @return current word
	 */
    public String getWord() {
		return word;
	}

	/**
	 *
	 * @param word word to set
	 */
	public void setWord(String word) {
		this.word = word;
	}

	/**
	 * @return current frequency
	 */

	public int getFrequency() {
		return frequency;
	}

	/**
	 *
	 * @param frequency frequency to set
	 */
	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}

	/**
	 * @return current keyword
	 */
	public int getKeyword() {
		return keyword;
	}

	/**
	 *
	 * @param keyword keyword to set
	 */
	public void setKeyword(int keyword) {
		this.keyword = keyword;
	}
}
