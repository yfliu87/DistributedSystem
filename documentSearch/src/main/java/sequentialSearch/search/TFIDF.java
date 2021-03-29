package sequentialSearch.search;

import sequentialSearch.model.DocumentData;

import java.util.*;
import java.util.stream.Collectors;

public class TFIDF {

    public static double calculateTermFrequency(List<String> words, String term) {
        long count = words.stream().filter(word -> word.equalsIgnoreCase(term)).count();
        return (double) count / words.size();
    }

    public static DocumentData createDocumentData(List<String> words, List<String> terms) {
        DocumentData documentData = new DocumentData();
        terms.forEach(term -> documentData.putTermFrequency(term, calculateTermFrequency(words, term)));
        return documentData;
    }

    public static Map<Double, List<String>> getDocumentsSortedByScore(List<String> terms, Map<String, DocumentData> documentResults) {
        TreeMap<Double, List<String>> scoreToDocuments = new TreeMap<>();
        Map<String, Double> termToInverseDocumentFrequency = getTermToInverseDocumentFrequencyMap(terms, documentResults);

        documentResults.keySet().forEach(document -> {
            DocumentData documentData = documentResults.get(document);
            double score = calculateDocumentScore(terms, documentData, termToInverseDocumentFrequency);
            addDocumentScoreToTreeMap(scoreToDocuments, score, document);
        });

        return scoreToDocuments.descendingMap();
    }

    private static double getInverseDocumentFrequency(String term, Map<String, DocumentData> documentResults) {
        double termCount = documentResults.values().stream().filter(documentData -> documentData.getTermFrequency(term) != 0.0).count();
        return termCount == 0 ? 0 : Math.log10(documentResults.size() / termCount);
    }

    private static Map<String, Double> getTermToInverseDocumentFrequencyMap(List<String> terms,
                                                                            Map<String, DocumentData> documentResults) {
        Map<String, Double> termToIDF = new HashMap<>();
        terms.forEach(term -> termToIDF.put(term, getInverseDocumentFrequency(term, documentResults)));
        return termToIDF;
    }

    private static double calculateDocumentScore(List<String> terms,
                                                 DocumentData documentData,
                                                 Map<String, Double> termToInverseDocumentFrequency) {
        return terms.stream().mapToDouble(term -> {
            double termFrequency = documentData.getTermFrequency(term);
            double inverseTermFrequency = termToInverseDocumentFrequency.get(term);
            return termFrequency * inverseTermFrequency;
        }).sum();
    }

    private static void addDocumentScoreToTreeMap(TreeMap<Double,
                                                  List<String>> scoreToDoc,
                                                  double score,
                                                  String document) {
        List<String> documentsWithCurrentScore = scoreToDoc.getOrDefault(score, new ArrayList<>());
        documentsWithCurrentScore.add(document);
        scoreToDoc.put(score, documentsWithCurrentScore);
    }

    public static List<String> getWordsFromLine(String line) {
        return Arrays.asList(line.split("(\\.)+|(,)+|( )+|(-)+|(\\?)+|(!)+|(;)+|(:)+|(/d)+|(/n)+"));
    }

    public static List<String> getWordsFromLines(List<String> lines) {
        return lines.stream()
                .map(line -> getWordsFromLine(line))
                .flatMap(list -> list.stream())
                .collect(Collectors.toList());
    }
}
