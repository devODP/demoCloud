package objects;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class StockQuote {

	public StockQuote() {
		super();
	}

	public String readHTML(String symbol) throws IOException {
		String html = "";
		URL dataSource = new URL("https://finance.yahoo.com/quote/" + symbol);
		URLConnection yc = dataSource.openConnection();
		BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));
		String inputLine;

		while ((inputLine = in.readLine()) != null) {
			html = html + inputLine;
		}

		in.close();

		if (html.contains("<title></title>")) {
			return null;
		} else {
			return html;
		}
	}

	// Given symbol, get current stock price.
	public String getPrice(String html) {
		// String html = "<p>An <a href='http://example.com/'><b>example</b></a>
		// link.</p>";

		Document doc = Jsoup.parse(html);
		Element link = doc.select("fin-streamer").first();

		// String text = doc.body().text(); // "An example link"
		String price = link.attr("value"); // "http://example.com/"
		// String linkText = link.text(); // "example""

		// String linkOuterH = link.outerHtml();
		// "<a href="http://example.com"><b>example</b></a>"
		// String linkInnerH = link.html(); // "<b>example</b>"

		return price;
	}

	public String getPreviousClose(String html) {
		Document doc = Jsoup.parse(html);
		Element link = doc.selectFirst("td[class=\"Ta(end) Fw(600) Lh(14px)\"][data-test=\"PREV_CLOSE-value\"]");
		if (link != null) {
			String previousClosedPrice = link.text();
			return previousClosedPrice;
		}
		return "na";
	}

	public String getDate(String html) {
		// String html = "<p>An <a href='http://example.com/'><b>example</b></a>
		// link.</p>";

		Document doc = Jsoup.parse(html);
		Element link = doc.select("div#quote-market-notice").first();

		// String text = doc.body().text(); // "An example link"
		String date = link.text(); // "http://example.com/"
		// String linkText = link.text(); // "example""

		// String linkOuterH = link.outerHtml();
		// "<a href="http://example.com"><b>example</b></a>"
		// String linkInnerH = link.html(); // "<b>example</b>"

		return date;
	}
}
