package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.query.OptimizeQuery;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class BzOptimizeQuery implements OptimizeQuery {
    private final Pattern patternQuery = Pattern.compile("^[a-zA-Z0-9_; ]*$");

    /**
     * Optimize sql query
     * @param query sql query
     * @return optimized sql query
     */
    @Override
    public String optimize(String query) {
        final String loweredQuery = query.toLowerCase();
        final StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < loweredQuery.length(); ++i) {
            final char c = loweredQuery.charAt(i);
            try {
                if (!patternQuery.matcher(Character.toString(c)).matches()) {
                    stringBuilder.append(" ")
                            .append(c)
                            .append(" ");
                } else {
                    stringBuilder.append(c);
                }
            } catch (PatternSyntaxException exception) {
                stringBuilder.append(" ")
                        .append(c)
                        .append(" ");
            }
        }
        return stringBuilder.toString().replaceAll(" {2,}", " ");
    }
}
