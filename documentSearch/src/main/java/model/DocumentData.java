package model;

import java.util.HashMap;
import java.util.Map;

public class DocumentData {
    private Map<String, Double> termToFrequency = new HashMap<>();

    public void putTermFrequency(String term, double frequency) {
        termToFrequency.put(term, frequency);
    }

    public double getTermFrequency(String term) {
        return termToFrequency.get(term);
    }

    public boolean containsTerm(String term) {
        return termToFrequency.containsKey(term);
    }

    @Override
    public String toString() {
        return this.termToFrequency.toString();
    }
}
