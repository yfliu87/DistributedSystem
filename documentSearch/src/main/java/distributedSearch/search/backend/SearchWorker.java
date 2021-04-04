package distributedSearch.search.backend;

import distributedSearch.model.backend.DocumentData;
import distributedSearch.model.backend.Result;
import distributedSearch.model.backend.SerializationUtils;
import distributedSearch.model.backend.Task;
import distributedSearch.network.OnRequestCallback;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static distributedSearch.utils.Params.TASK_ENDPOINT;

public class SearchWorker implements OnRequestCallback {

    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        Task task = (Task) SerializationUtils.deserialize(requestPayload);
        Result result = createResult(task);
        return SerializationUtils.serialize(result);
    }

    private Result createResult(Task task) {
        List<String> documents = task.getDocuments();

        Result result = new Result();

        documents.forEach(document -> {
            List<String> words = parseWordsFromDocument(document);
            DocumentData documentData = TFIDF.createDocumentData(words, task.getSearchTerms());
            result.addDocumentData(document, documentData);
        });

        return result;
    }

    private List<String> parseWordsFromDocument(String document) {
        try {
            FileReader fileReader = new FileReader(document);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            List<String> lines = bufferedReader.lines().collect(Collectors.toList());
            List<String> words = TFIDF.getWordsFromLines(lines);
            return words;
        } catch (FileNotFoundException e) {
            return Collections.emptyList();
        }

    }

    @Override
    public String getEndpoint() {
        return TASK_ENDPOINT;
    }
}
