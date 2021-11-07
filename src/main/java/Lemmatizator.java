import org.apache.lucene.morphology.LuceneMorphology;
import org.apache.lucene.morphology.english.EnglishLuceneMorphology;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class Lemmatizator {
    private LuceneMorphology englishMorphology;
    private LuceneMorphology russianMorphology;

    public Lemmatizator() {
        try {
            englishMorphology = new EnglishLuceneMorphology();
            russianMorphology = new RussianLuceneMorphology();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<String, Integer> getLemma(String input) throws IOException {
        LuceneMorphology lm;

        Map<String, Integer> result = new HashMap<>();
        String[] split = input.split("[\\s]+");
        for (String s : split) {
            lm = getMorphology(s);
            if (lm == null) continue;
            List<String> normalForms = lm.getNormalForms(s.toLowerCase());
            if (result.containsKey(normalForms.get(0)) && checkWord(normalForms.get(0))) {
                Integer count = result.get(normalForms.get(0));
                result.put(normalForms.get(0), ++count);
            } else if (checkWord(normalForms.get(0))) {
                result.put(normalForms.get(0), 1);
            }
        }
        return result;
    }

    public String getOneLemma(String input) throws IOException {
        LuceneMorphology lm = getMorphology(input);
        List<String> normalForms = lm.getNormalForms(input.toLowerCase());
        if (checkWord(normalForms.get(0))) {
            return normalForms.get(0);
        } else {
            return "";
        }
    }

    private boolean checkWord(String input) throws IOException {
        LuceneMorphology lm = getMorphology(input);
        if (lm == null) return false;
        List<String> morphInfo = lm.getMorphInfo(input);
        String[] partOfSpeech = morphInfo.get(0).split(" ");
        if (partOfSpeech[1].matches("СОЮЗ|ПРЕДЛ|ЧАСТ|МЕЖД")) {
            return false;
        }
        return true;
    }

    private LuceneMorphology getMorphology(String input) throws IOException {
        Pattern russian = Pattern.compile("^[а-яА-ЯёЁ]+$");
        Pattern english = Pattern.compile("^[a-zA-Z]+$");
        if (russian.matcher(input).find()) {
            return russianMorphology;
        } else if (english.matcher(input).find()) {
            return englishMorphology;
        } else {
            return null;
        }
    }
}
