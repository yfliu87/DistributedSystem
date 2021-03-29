package sequentialSearch;

import sequentialSearch.model.DocumentData;
import sequentialSearch.search.TFIDF;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SequentialSearch {
    public static final String BOOKS_DIRECTORY = "/Users/yifeiliu/Documents/DistributedSystem/DistributedSystem/documentSearch/src/main/resources/books";
    public static final String SEARCH_QUERY_1 = "The best detective that catches many criminals using his deductive methods";
    public static final String SEARCH_QUERY_2 = "The girl that falls through a rabbit hole into a fantasy wonderland";
    public static final String SEARCH_QUERY_3 = "A war between Russia and France in the cold winter";

    public static void main(String[] args) throws FileNotFoundException {
        File documentsDirectory = new File(BOOKS_DIRECTORY);

        List<String> documents = Arrays.asList(documentsDirectory.list())
                .stream()
                .map(documentName -> BOOKS_DIRECTORY + "/" + documentName)
                .collect(Collectors.toList());

        for (String query : Arrays.asList(SEARCH_QUERY_1, SEARCH_QUERY_2, SEARCH_QUERY_3)) {
            System.out.println("Query: " + query);

            List<String> terms = TFIDF.getWordsFromLine(query);
            findMostRelevantDocuments(documents, terms);
        }
    }

    private static void findMostRelevantDocuments(List<String> documents, List<String> terms) throws FileNotFoundException {
        Map<String, DocumentData> documentsResults = new HashMap<>();

        for (String document : documents) {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(document));
            List<String> lines = bufferedReader.lines().collect(Collectors.toList());
            List<String> words = TFIDF.getWordsFromLines(lines);
            DocumentData documentData = TFIDF.createDocumentData(words, terms);
            documentsResults.put(document, documentData);
        }

        Map<Double, List<String>> documentsByScore = TFIDF.getDocumentsSortedByScore(terms, documentsResults);
        printResults(documentsByScore);
    }

    private static void printResults(Map<Double, List<String>> documentsByScore) {
        for (Map.Entry<Double, List<String>> entry : documentsByScore.entrySet()) {
            for (String doc : entry.getValue()) {
                System.out.println(String.format("Most relevant doc score: %f, doc: %s\n", entry.getKey(), doc));
                break;
            }
            break;
        }
    }
}
