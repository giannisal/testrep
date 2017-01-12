package parsing;

import static org.junit.Assert.*;
import org.junit.Test;


public class WordsUrlParsingTest {

	@Test
	public void testRemoveTags() {
		WordsUrlParsing w = new WordsUrlParsing();

		assertEquals("", w.removeTags("\"<\""));
		
		assertEquals("", w.removeTags("\">\""));

		assertEquals(" ", w.removeTags("<!--a-->"));
		assertEquals(" ", w.removeTags("<script>a</script>"));
		assertEquals(" ", w.removeTags("<style>a</style>"));
		assertEquals(" ", w.removeTags("<iframe>a</iframe>"));

		assertEquals(" ", w.removeTags("<a>"));
		assertEquals(" b ", w.removeTags("<a>b<a>"));
		assertEquals("b  ", w.removeTags("b<a><a>"));
		assertEquals(" b", w.removeTags("<a>b"));

		assertEquals(" ", w.removeTags("!"));
		assertEquals(" ", w.removeTags("1"));
	}

	@Test
	public void testReturnTitle() {
		assertEquals("Hello", WordsUrlParsing.returnTitle("<title>Hello</title>"));
		assertEquals("Hello", WordsUrlParsing.returnTitle("a<title>Hello</title>a"));
		assertEquals("Hello", WordsUrlParsing.returnTitle("<a><title>Hello</title><a>"));
	}

	@Test
	public void testModifyString() {
		assertEquals(" ", WordsUrlParsing.modifyString("&entity;"));
		assertEquals("a ", WordsUrlParsing.modifyString("a&entity;"));
		assertEquals(" a", WordsUrlParsing.modifyString("&entity;a"));
		assertEquals("< >", WordsUrlParsing.modifyString("<&entity;>"));
	}

	@Test
	public void testReturnDescription() {
		String input = "<meta name=\"description\" content=\"description\"/>";
		String output = "description  ";
		assertEquals(output, WordsUrlParsing.returnDescription(input));

		input = "<meta content=\"description\" name=\"description\"/>";
		output = "description   ";
		assertEquals(output, WordsUrlParsing.returnDescription(input));

		input = "a<meta name=\"description\" content=\"description\"/>a";
		output = "description  ";
		assertEquals(output, WordsUrlParsing.returnDescription(input));

		input = "<meta><meta name=\"description\" content=\"description\"/>";
		output = "description  ";
		assertEquals(output, WordsUrlParsing.returnDescription(input));

		input = "<meta name=\"description\">";
		output = " ";
		assertEquals(output, WordsUrlParsing.returnDescription(input));

		input = "<meta name=\"keywords\" content=\"description\">";
		output = null;
		assertEquals(output, WordsUrlParsing.returnDescription(input));

		input = "<meta name=\"keywords\" content=\"description\"><meta name=\"description\" content=\"description\">";
		output = "description ";
		assertEquals(output, WordsUrlParsing.returnDescription(input));
	}

	@Test
	public void testReturnKeywords() {
		String input = "<meta name=\"keywords\" content=\"keyword\" /> ";
		String[] output1 = { "keyword" };
		assertArrayEquals(output1, WordsUrlParsing.returnKeywords(input));

		input = "<meta name=\"keywords\" content=\"keyword, keyword\"/> ";
		String[] output2 = { "keyword", "keyword" };
		assertArrayEquals(output2, WordsUrlParsing.returnKeywords(input));

		input = "<meta /><meta name=\"keywords\" content=\"keyword\" />";
		String[] output3 = { "keyword" };
		assertArrayEquals(output3, WordsUrlParsing.returnKeywords(input));

		input = "a<meta name=\"keywords\" content=\"keyword\" /> ";
		String[] output4 = { "keyword" };
		assertArrayEquals(output4, WordsUrlParsing.returnKeywords(input));
	}
}