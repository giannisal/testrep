/**
* The Sites program implements the initialization of id_site,site,title and description
* variables and the methods to set and return these variables.
*
* @author  J0arTeam
* @version 1.0
* @since   2017-01-03
*/
package joarLib;

public class Sites {
 /** Η μεταβλητή με έναν αύξοντα αριθμό για τα site*/
  private int id_site;
  /** Η μεταβλητή με το url του site*/
  private String site;
  /** Η μεταβλητή με τον τίτλο του site που δίνεται από την html*/
  private String title;
  /** Η μεταβλητή με την περιγραφή του site που δίνονται από τα <meta> tags της html */
  private String description;


   /** Constructor to initiliaze the id_site,site,title and description variables*/
	public Sites(int id_site, String site, String title, String description) {
		this.id_site = id_site;
		this.site = site;
		this.title = title;
		this.description = description;
	}
	/** Constructor to initiliaze the site,title and description variables*/
	public Sites(String site , String title, String description) {
		this.site = site;
		this.title = title;
		this.description = description;
    }

 /**Consructor to initialize only the site variable*/
  public Sites(String site) {
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
	 * @return current title
	 */
  public String getTitle() {
		return title;
  }

	/**
	 *
	 * @param title title to set
	 */
	public void setTitle(String title) {
		this.title = title;
	}

	/**
	 * @return current description
	 */
  public String getDescription() {
    return description;
  }

	/**
	 *
	 * @param description description to set
	 */
  public void setDescription(String description) {
    this.description = description;
  }

	/**
	 * @return current id_site
	 */
	public int getIdSite() {
		return id_site;
	}

	/**
	 *
	 * @param id_site id_site to set
	 */
	public void setIdSite(int id_site) {
		this.id_site = id_site;
	}
}
