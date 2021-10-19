import org.jsoup.Connection;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Pattern;

public class LinkParser extends RecursiveAction {
    private final String url;
    private static String mainUrl = "";
    private static int count = 0;
    private static final Set<String> handledUrls = new HashSet<>();

    public LinkParser(String url) {
        if (count == 0) {
            mainUrl = url;
            handledUrls.add(mainUrl);
            count++;
        }
        this.url = url;
    }

    protected void compute() {
        List<LinkParser> linkParserList = new ArrayList<>();
        Document currentDoc = getPageDocument(url);
        Set<String> childrens = getChildrenLink(currentDoc);
        for (String child : childrens) {

            LinkParser task = new LinkParser(child);

            task.fork();

            linkParserList.add(task);
        }
        linkParserList.forEach(ForkJoinTask::join);

        try {
            if (!currentDoc.text().isEmpty()) {
                Map<String, Integer> wordsFromTitle = getWords(currentDoc.select("title"));
                Map<String, Integer> wordsFromBody = getWords(currentDoc.select("body"));
                String urlToDB = url.replaceAll(mainUrl, "");
                Connection.Response execute = Jsoup.connect(url).execute();
                int statusCodeToDB = execute.statusCode();
                int currentPageId;
                currentPageId = DBConnection.insertToPage(urlToDB, statusCodeToDB, currentDoc.toString());
                if (currentPageId != 0) {
                    String bigSql = generateBigSql(wordsFromTitle, currentPageId);
                    DBConnection.multiInsertOrUpdateToIndex(bigSql);
                    bigSql = generateBigSql(wordsFromBody, currentPageId);
                    DBConnection.multiInsertOrUpdateToIndex(bigSql);
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

    }

    private Set<String> getChildrenLink(Document document) {
        Set<String> links = new HashSet<>();
        Elements aTags = document.select("a");
        for (Element aTag : aTags) {
            String currentHref = aTag.absUrl("href");
            if (getCheckUrl(currentHref)) {
                handledUrls.add(currentHref);
                links.add(currentHref);
            }
        }

        return links;
    }

    private boolean getCheckUrl(String urlCheck) {
        Pattern pattern = Pattern.compile("(png|pdf|jpg|gif|#)");

        return (urlCheck.startsWith(url) || urlCheck.startsWith(mainUrl)) &&
                !handledUrls.contains(urlCheck) && !pattern.matcher(urlCheck).find();
    }

    private Document getPageDocument(String url) {
        try {
            Document document = Jsoup.connect(url)
                    //  .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) Gecko/20070725 Firefox/2.0.0.6")
                    //  .referrer("http://www.google.com")
                    .ignoreHttpErrors(false)
                    .timeout(10 * 1000)
                    .ignoreContentType(true)
                    .get();
            Thread.sleep(500);
            return document;
        } catch (HttpStatusException h) {
            String errorUrl = h.getUrl();
            if (errorUrl.startsWith(mainUrl)) {
                errorUrl = errorUrl.replaceAll(mainUrl, "");
            }
            DBConnection.insertToPage(errorUrl, h.getStatusCode(), h.getLocalizedMessage());
        } catch (IOException | InterruptedException e) {
            System.out.println(e.getMessage() + " URL " + url);
        }
        return new Document("");
    }

    private String generateBigSql(Map<String, Integer> wordsFromPage, int pageId) {
        int tmp = wordsFromPage.keySet().size();
        StringBuffer bigSql = new StringBuffer();
        for (String s : wordsFromPage.keySet()) {
            Integer count = wordsFromPage.get(s);
            int currentLemmaId = DBConnection.insertOrUpdateToLemma(s, count);
            tmp--;
            float rank = (float) (count * 0.8);
            bigSql.append("(");
            bigSql.append(pageId);
            bigSql.append(", ");
            bigSql.append(currentLemmaId);
            bigSql.append(", ");
            bigSql.append(rank);
            bigSql.append(tmp == 0 ? ")" : "), ");
        }
        return bigSql.toString();
    }

    private Map<String, Integer> getWords(Elements elements) {
        Map<String, Integer> lemma = new HashMap<>();
        for (Element element : elements) {
            String newS = element.text().replaceAll("[^А-Яа-яёЁA-Za-z]+", " ").trim();
            Map<String, Integer> currentWords = null;
            try {
                currentWords = Lemmatizator.getLemma(newS);
            } catch (IOException e) {
                e.printStackTrace();
            }
            lemma.putAll(currentWords);
        }
        return lemma;
    }

}
