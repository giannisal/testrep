<%@ page language="java" contentType="text/html; charset=ISO-8859-7" pageEncoding="UTF-8"%>
<%@ page import="java.util.*, javax.servlet.*, java.io.*, joarLib.Joar_DB, joarLib.Siteword"%>

<%
String url = "http://senja.gr/alumni/";
URL oracle = new URL("http://senja.gr/alumni/");
BufferedReader in = new BufferedReader(
new InputStreamReader(oracle.openStream()));

String input, texturl="";
while ((input = in.readLine()) != null)
texturl += input;
in.close();

String str = "", str2 = "", str3 = "", str4 = "", str5 = "";
boolean check = false, script = false, comment = false, style = false;
int special = 2, len3 = 0, len4 = 0, len5 = 0;
String[] words;

for (char ch: texturl.toCharArray()) {
  if (comment == true) {
    str4 += ch;
    len4 = str4.length();
    if (len4 > 4 && str4.substring(len4-3, len4).equals("-->")) {
      comment = false;
      str4 = "";
      len4 = 0;
    }
  }
  if (script == true && comment == false) {
    str3 += ch;
    len3 = str3.length();
    if (len3 > 7 && str3.substring(len3-8, len3).equals("/script>")) {
      script = false;
      str3 = "";
      len3 = 0;
    }
  }
  if (style == true && comment == false) {
    str5 += ch;
    len5 = str5.length();
    if (len5 > 6 && str5.substring(len5-7, len5).equals("/style>")) {
      style = false;
      str5 = "";
      len5 = 0;
    }
  }
  if (ch=='>' && special%2 == 0 && comment == false && script == false && style == false) {
    if (str2.indexOf("script") == 0) {
      script = true;
    }
    if (str2.indexOf("style") == 0) {
      style = true;
    }
    check = true;
    str2 = "";
  } else if ((ch =='-') && (str2.indexOf("!-") == 0) && comment == false) {
      comment = true;
  }
  else if (ch=='<' && comment == false && script == false && style == false) {
    if (str!="") {
      words = str.split("\\s+");
      for (String word : words) {
        if (!word.equals("") && !word.substring(word.length() - 1, word.length()).equals(";")) {
          // System.out.println(word);
          Joar_DB database = new Joar_DB();
          database.open();
          database.importSiteword(url, word);
          database.close();
        }
      }
    }
    check=false;
    str = "";
  } else if (check==true && script==false && comment == false && style == false) {
    str += ch;
  } else if (check==false && comment == false) {
    str2 += ch;
  }
  if (ch=='\"') {
    special++;
  }
}
} catch(Exception e1) {

}
%>

  <%-- String url = "www.wordreference.com";
  String texturl = readFile("ismgroup3/joar/word.txt");
  String[] words = texturl.split("\\.|\\,|\\:|\\||\\?|\\$|\\^|\\*|\\+|\\(|\\)|\\{|\\(|\\s+");

  for (String word : words) {
     Joar_DB database = new Joar_DB();
     database.open();
     database.importSiteword(url, word);
     database.close();
  }

  response.sendRedirect("index.jsp"); --%>
