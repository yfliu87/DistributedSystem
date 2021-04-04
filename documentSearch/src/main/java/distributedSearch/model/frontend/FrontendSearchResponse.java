package distributedSearch.model.frontend;

import java.util.Collections;
import java.util.List;

public class FrontendSearchResponse {
    private List<SearchResultInfo> searchResults = Collections.emptyList();

    public FrontendSearchResponse(List<SearchResultInfo> searchResults) {
        this.searchResults = searchResults;
    }

    public List<SearchResultInfo> getSearchResults() {
        return searchResults;
    }

    public static class SearchResultInfo {
        private String title;
        private String extension;
        private int score;

        public SearchResultInfo(String title, String extension, int score) {
            this.title = title;
            this.extension = extension;
            this.score = score;
        }

        public String getTitle() {
            return title;
        }

        public String getExtension() {
            return extension;
        }

        public int getScore() {
            return score;
        }
    }
}
